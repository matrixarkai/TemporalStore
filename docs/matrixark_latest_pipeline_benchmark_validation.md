# MatrixArk Latest Pipeline And Benchmark Validation

Date: 2026-06-22

## What Was Validated

The latest MatrixArk context features were validated end to end across the local Python adapter and live C++ TemporalStore direct SDK path.

Validated MatrixArk features:

- lightweight single-message ingest
- session buffering
- threshold and hook-boundary batch extraction
- one-pass batch extraction
- multi-segment memory extraction
- ContextEvent, ContextEntity, ContextSegment, ContextSummary, ContextEmbedding, ContextIndex, ContextPackAudit
- async ContextNode L0/L1 summary refresh
- every refreshed ContextNode prefix has both L0/L1 summaries and L0/L1 embeddings
- tree-first retrieval using node summary embeddings
- secondary-index filtering before embedding scoring
- current-state entity updates with deterministic patching
- temporal/date query planning for before/after questions
- weighted recall with dense, sparse lexical, time decay, and business score
- TIME_COMPRESS operator path
- local/remote context budget dedupe
- Codex hook lifecycle paths

## Validation Commands

Unit and guard tests:

```bash
PYTHONPATH=. python3 -m unittest \
  tools.test_matrixark_mcp_server \
  tools.test_matrixark_codex_hook \
  tools.test_matrixark_full_dataset_cpp_guard
```

Result:

```text
Ran 36 tests in 3.005s
OK
```

Feature scripts:

```bash
PYTHONPATH=. python3 tools/run_matrixark_one_pass_batch_extract_test.py
PYTHONPATH=. python3 tools/run_matrixark_memory_segmentation_test.py
PYTHONPATH=. python3 tools/run_matrixark_entity_update_algorithm_test.py
PYTHONPATH=. python3 tools/run_matrixark_weighted_recall_test.py
PYTHONPATH=. python3 tools/run_matrixark_operator_compression_test.py
```

Result: all scripts returned `status: passed`.

## LOCOMO-Style Debug Flow

Command:

```bash
PYTHONPATH=. python3 tools/run_matrixark_locomo_debug_flow.py \
  > /tmp/matrixark_locomo_debug_flow_latest.json
```

This is a LOCOMO-style debug flow, not an official LOCOMO dataset run.

Observed records:

```json
{
  "context_batch_commit": 3,
  "context_embedding": 90,
  "context_entity": 9,
  "context_entity_update_audit": 9,
  "context_event": 12,
  "context_extraction_audit": 3,
  "context_index": 24,
  "context_pack_audit": 9,
  "context_segment": 6,
  "context_summary": 63,
  "context_summary_dirty": 51,
  "context_summary_refresh_audit": 24,
  "matrixark_audit_log": 27,
  "session_buffer_event": 12
}
```

Important retrieval checks:

- Current location query returns the latest `ContextEntity`: Austin.
- Current preference query returns the latest preference entity plus raw event evidence.
- Approval query returns approval entity, raw event, and segment evidence.
- Historical `before April 10` query returns raw dated events plus latest entity state.
- Tree traversal is active with `fallback_to_flat=false` after async summary refresh.

## C++ TemporalStore Direct SDK Validation

Local C++ TemporalStore was started with release binaries:

```bash
OUT_DIR=<repo>/output-ubuntu22/release \
  bash tools/deploy_local_ubuntu22.sh start
```

Result:

```text
TemporalStore local deployment is running
metaserver leader: 127.0.0.1:18000
server1: 127.0.0.1:18001
```

Direct SDK E2E:

```bash
PYTHONPATH=. python3 tools/run_matrixark_temporalstore_direct_e2e.py \
  --temporalstore-lib output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_cpp_direct_e2e_latest.json
```

Result:

```json
{
  "backend": "temporalstore-direct",
  "status": "passed",
  "stored_record_count": 31,
  "first_retrieve_selected": 1,
  "second_retrieve_selected": 1,
  "ingest_classifications": ["NEW_EVENT", "NEW_EVENT", "CONFIRMATION"]
}
```

C++ storage-backed scale check:

```bash
PYTHONPATH=. python3 tools/run_matrixark_context_storage_benchmark.py \
  --backend temporalstore-direct \
  --temporalstore-lib output-ubuntu22/release/sdk/lib/libbcache2.so \
  --events 40 \
  --queries 8 \
  --batch-size 20 \
  --restart-before-query
```

Result:

```json
{
  "backend": "temporalstore-direct",
  "status": "passed",
  "messages_ingested": 40,
  "batches": 2,
  "queries": 8,
  "hit_rate": 1.0,
  "ingest_mode": "batch",
  "restart_before_query": true,
  "ingest_latency_ms": {"avg": 602.725, "p50": 448.481, "p95": 756.969},
  "retrieve_latency_ms": {"avg": 57.738, "p50": 17.786, "p95": 345.04}
}
```

## LOCOMO And LongMemEval Status

Full official LOCOMO and LongMemEval_s were not run in this validation because the official dataset source files are not present locally in the expected runner format.

The full-dataset C++ guard was validated in preflight mode:

```bash
python3 tools/run_matrixark_full_dataset_cpp_benchmark.py \
  --dataset locomo \
  --artifact-dir /tmp/matrixark_missing_artifacts \
  --artifact-prefix local-check \
  --allow-missing-artifacts \
  --validate-only

python3 tools/run_matrixark_full_dataset_cpp_benchmark.py \
  --dataset longmemeval_s \
  --artifact-dir /tmp/matrixark_missing_artifacts \
  --artifact-prefix local-check \
  --allow-missing-artifacts \
  --validate-only
```

Result: both require `temporalstore-direct` and canonical artifacts:

- `result.json`
- `report.json`
- `report.md`
- `hypotheses.jsonl`
- `context_packs.jsonl`
- `judge.jsonl`

## Fixes Added In This Validation Pass

- Evolving entities now generate deterministic patches for location, job/status, plan, family/profile, relationship, approval, confirmation, correction, and preference.
- Batch extraction de-duplication now keeps the latest canonical entity mention in a batch.
- LOCOMO debug flow now runs `matrixark_refresh_summaries` before retrieval so tree-first traversal is validated.
- Regression coverage now asserts all path prefixes for a refreshed node have `node_l0` and `node_l1` summaries and embeddings.
- Secondary-index filtering no longer lets unrelated entities inherit all batch index terms.
- Multi-intent raw queries use `any_group` secondary filter mode so questions asking for multiple memory types can retrieve each type.
- Temporal/date query planning now covers `before`, `after`, `as of`, and `valid as of`.
- Historical temporal questions allow raw message events through the relevant filter and boost dated raw events in packing.

## Remaining Benchmark Gaps

- Download/prepare official LOCOMO in the runner's expected array format.
- Download/prepare official LongMemEval_s in the runner's expected array format.
- Run both full datasets with C++ TemporalStore direct SDK.
- Run OSS or OpenAI-compatible reader/judge; deterministic reader remains CI/debug only.
- Save canonical artifacts for every official run.
- Compare MatrixArk numbers separately from VikingMem paper numbers until dataset, reader, judge, prompt, and scoring protocol match.
