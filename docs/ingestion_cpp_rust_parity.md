# Ingestion C++ Rust Parity Contract

This shared contract covers ingestion behavior that should be executable in both C++ and Rust.

Unified cases should validate:

- Kafka consumer group runtime and partition assignment
- streaming ingestion micro-batches with ordered stream sequence fences
- stream replay duplicate rejection and active in-flight backpressure
- batch ingestion with mixed API, Kafka, and Flink records plus per-record status
- durable topic/partition/offset ledger
- rebalance-required and backpressure reporting
- Flink checkpoint precommit, commit, and abort lifecycle
- dead-letter capture/export without blocking valid records
- lag metrics and high-watermark reporting
- restart and Raft leader-change idempotence

Reports should include source id, partition, offset/checkpoint id, stream id,
start sequence, committed sequence, status, retry count, lag, backpressure
reject count, and dead-letter counts so C++ and Rust output can be compared
case by case.
