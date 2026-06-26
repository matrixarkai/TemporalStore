# Ingestion C++ Rust Parity Contract

This shared contract covers ingestion behavior that should be executable in both C++ and Rust.

Unified cases should validate:

- Kafka consumer group runtime and partition assignment
- durable topic/partition/offset ledger
- rebalance-required and backpressure reporting
- Flink checkpoint precommit, commit, and abort lifecycle
- dead-letter capture/export without blocking valid records
- lag metrics and high-watermark reporting
- restart and Raft leader-change idempotence

Reports should include source id, partition, offset/checkpoint id, status, retry count, lag, and
dead-letter counts so C++ and Rust output can be compared case by case.
