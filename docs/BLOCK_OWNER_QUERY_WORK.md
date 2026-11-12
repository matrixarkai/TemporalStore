# Block Ownership Query Work

Container mutation asks whether another member still owns a physical block
before retiring that block. Hash fields and ordered member maps now compare the
physical slab, offset and length before comparing the member bytes. Members in
other blocks cannot affect the answer and no longer pay for a byte comparison.
The excluded member is still compared by value, not allocation identity.

The change adds no resident index and changes no persisted fields, placement
rules or durability fences. A negative query still visits every member, so this
is not a constant-time ownership lookup.

Three focused tests cover value-equal keys in separate allocations, the full
physical block tuple, shared blocks, hash fields, scored values, list positions
and member churn. The counted-key fixture visits up to 4,096 members: its
key-first predicate compares 4,096 keys for a negative query, while the
address-first predicate compares one for the excluded block or zero for an
absent block.

An exact-function Callgrind probe used 512 negative queries over 4,096 members
with 512-byte keys, with identical false answers. In one x86-64 debug test build
with `temporal-raft-engine,alloc-probe` enabled,
key-first work counted 1,406,274,612 instructions and address-first work counted
1,087,649,842, about 22.7% less. Collection covered each query-workload function,
including the same query-row preparation, but excluded map construction and the
test framework. This is instruction work, not cycles, wall latency, RSS or
large-corpus API acceptance.

```bash
cargo test -p temporalstore-rust --lib block_owner_query_tests \
  -- --test-threads=1 --nocapture
MATRIXARK_OWNER_QUERY_VARIANT=key-first cargo test -p temporalstore-rust --lib \
  --features temporal-raft-engine,alloc-probe \
  block_owner_query_cpu_fixture -- --ignored --test-threads=1 --nocapture
MATRIXARK_OWNER_QUERY_VARIANT=address-first cargo test -p temporalstore-rust --lib \
  --features temporal-raft-engine,alloc-probe \
  block_owner_query_cpu_fixture -- --ignored --test-threads=1 --nocapture
```

The ignored fixture does not enforce a timing threshold. CI requires all three
functional checks to run and pass; a missing or renamed filter fails the gate.
The broader Mem0 serving workload and real-model extraction remain separate
acceptance work.

A subsequent storage-only diagnostic acknowledged all 16,384 records but logged
a batch metadata request at 205,353 microseconds before debugger attachment,
exceeding its 200 ms limit. The capture showed a request filtering and cloning
block lookups during hash metadata installation, with maintenance asleep. It
does not establish what caused the earlier slow request. Other Rust builds were
active, so this is not an isolated latency result. Rust-source fingerprints
were unchanged during the local test and serving builds. The diagnostic was
interrupted after capture and produced no complete endpoint acceptance report.
