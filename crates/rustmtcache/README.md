# rustmtcache

`rustmtcache` is the standalone Rust multi-tier cache library extracted from
TemporalStore. Its purpose is to evolve toward C++ mtcache feature and
functionality parity while remaining reusable by TemporalStore and other Rust
services.

The first exported surface keeps the existing TemporalStore cache behavior:
DRAM, PMEM-like resident tier, SSD/file tier, admission policy, pinned handles,
read-through refill, invalidation, async writeback/backpressure counters, and
cache pressure helpers.
