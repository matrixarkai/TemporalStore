<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# WAL Sequence Reads

The append path reads the durable log tail to reconcile sequence numbers, including writes
made by another store instance. Tail readers need the sequence and the physical record end,
not replay outcomes or rebuilt block identities.

Sequence-only decoding keeps the existing physical record reader, continuation handling,
frame checksum, decompression, complete protobuf decoding, and command and metadata
conversions. It skips only the infallible conversion of decoded outcomes and staged blocks
into replay structures. Recovery still uses the complete record decoder.

No sequence cache, file format, append lock, durability barrier, roll policy, reservation,
compaction fence, or corruption policy changes. The reader still reads and validates records;
this is not a constant-time tail lookup or a guarantee of lower endpoint latency.

Run the focused checks from the repository root:

```bash
cargo test -p temporalstore-rust --lib --features alloc-probe wal::sequence_read_tests:: -- --test-threads=1 --nocapture
```

The checks compare both decoders across all payload encodings, absent and present commands,
malformed embedded commands, truncated protobufs, malformed trailing outcomes, truncated
frames, byte mutations, encoding failures, and zero reservations. The allocation check
measures requested Rust allocation bytes for a 1,024-outcome fixture, not process RSS.
End-to-end Mem0 performance must be measured separately with the TemporalStore scale runner.
