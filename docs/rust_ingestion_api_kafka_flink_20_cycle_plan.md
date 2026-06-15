# Rust Ingestion API/Kafka/Flink Gap Fill Plan

Goal: close ingestion gaps without adding fake connector noops. Each cycle should land a concrete,
locally testable ingestion behavior that calls TemporalStore internals or validates production
connector contracts.

## 20-Cycle Backlog

1. Add Rust-native ingestion envelopes for API, Kafka, and Flink sources, normalize them into
   engine batch execution, validate offsets/checkpoints, and report per-record results. Done.
2. Expose `/ingest/batch` on nodeserver with direct engine execution and per-record reporting.
   Done.
3. Add proxy/table ingestion route that resolves table topology and splits records by shard.
4. Add Kafka source config structs, consumer group metadata, and offset commit planning reports.
5. Add Kafka idempotency ledger by topic/partition/offset with bounded retention. Done.
6. Add Flink checkpoint barrier metadata and checkpoint-aligned commit reports. Done.
7. Add ingestion dead-letter report format for malformed records and engine failures. Done.
8. Add ingestion backpressure/admission integration with shard/table/tenant QPS limits.
9. Add feature/sequence-specific ingestion builders matching C++ feature schemas.
10. Add JSONL file ingestion harness for local replay and connector simulation.
11. Add Kafka-like local partition harness that replays offsets through `/ingest/batch`.
12. Add Flink-like checkpoint harness with restart/replay and exactly-once assertions.
13. Add ingestion metrics for accepted, failed, duplicate, lag, and checkpoint age. Done.
14. Add ingestion recovery after node restart using persisted offset/checkpoint ledger. Done.
15. Add proxy route-cache refresh on ingestion shard mismatch.
16. Add ingestion scale harness mixed with Raft failover and shared-store comparison.
17. Add schema/version validation for API/Kafka/Flink payloads.
18. Add ingestion auth/tenant policy hooks for API and connector identities.
19. Add operational inspection APIs for connector lag, dead letters, and checkpoint status.
20. Final local gate covering API, Kafka-like, and Flink-like ingestion under restart/failover.

## Cycle 1 Implemented

Cycle 1 creates the shared ingestion contract:

- `IngestionSource` identifies API request ids, Kafka topic/partition/offset, and Flink
  job/operator/subtask/checkpoint/record positions.
- `IngestionBatchRequest` carries source-tagged TemporalStore commands.
- `TemporalEngine::ingest_batch` validates source metadata, detects duplicate Kafka offsets within
  the batch, and executes accepted records through the existing engine batch path.
- `IngestionBatchReport` returns accepted, failed, duplicate, and per-record statuses.
- Regression coverage proves API, Kafka, and Flink records mutate engine state, and duplicate Kafka
  offsets are rejected without nooping valid records.

The next cycle should expose this through the nodeserver HTTP surface so ingestion callers do not
need direct engine access.

## Cycle 2 Implemented

Cycle 2 exposes ingestion through the nodeserver HTTP surface:

- `/ingest/batch` now parses `IngestionBatchRequest` and executes accepted records through
  `TemporalEngine::ingest_batch`.
- C++-style aliases `/ServerService/IngestBatch` and `/IngestionService/IngestBatch` share the
  same implementation and response contract.
- Route coverage proves API, Kafka, and Flink-sourced records mutate TemporalStore internals rather
  than returning a noop success.
- REST helper coverage proves duplicate Kafka topic/partition/offset records are rejected while the
  first valid record still commits.

The next cycle should add proxy/table ingestion routing so callers can ingest by table/routing key
without precomputing the destination shard id.

## Cycle 3 Implemented

Cycle 3 adds durable local ingestion state for API/Kafka/Flink parity testing:

- Kafka offsets are persisted by topic and partition under the engine index directory.
- Replayed Kafka offsets at or below the committed offset are rejected before command execution and
  recorded as dead letters.
- Flink checkpoint updates support precommit, commit, and abort state transitions.
- Ingestion reports now include Kafka ledger entries, Flink checkpoint states, dead letters, max
  Kafka lag, and state persistence status.
- `GET /ingest/state`, `IngestionService/GetState`, and `ServerService/GetIngestionState` expose
  the persisted ingestion state.
- Prometheus metrics report accepted, failed, duplicate, dead-letter, Kafka committed, max lag,
  ledger count, and Flink checkpoint state counters.
- Regression coverage validates ledger persistence across engine restart, duplicate rejection
  before execution, dead-letter retention, lag reporting, and Flink checkpoint commit.

The next cycle should add proxy/table ingestion routing so callers can ingest by table/routing key
without precomputing the destination shard id, then mix ingestion into the Raft failover harness.
