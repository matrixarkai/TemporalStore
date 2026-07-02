# Streaming And Batch Ingestion Parity

This shared C++/Rust case covers product-visible ingestion behavior that is
separate from the Kafka/Flink offset-ledger case.

Required report fields:

- `stream_id`
- `start_sequence`
- `committed_sequence`
- `duplicate_count`
- `backpressure_rejected_count`
- batch accepted and failed counts
- Kafka committed offsets
- Flink checkpoint states
- dead-letter count
- lag and Prometheus/state-report visibility

Acceptance:

- streaming ingestion commits only ordered records within the in-flight limit
- replayed stream sequences are rejected before duplicate writes
- stream backpressure rejects overflow records without blocking valid records
- batch ingestion continues to accept mixed API, Kafka, and Flink records
- committed stream sequence is durable across restart
