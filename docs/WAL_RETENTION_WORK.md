# WAL Retention Scan Work

Index publication checks which physical write-ahead-log frames are still live
before proposing locator retirement. The scan now borrows keys and addresses
from the existing model visitor and inserts their folded identifiers directly
into one set. It no longer builds an owned live-block list or an intermediate
set of full object/address tuples.

The visitor still covers every live model kind, including kinds that do not
normally create physical frame registrations. Both synthetic WAL address forms
remain live when real frames are excluded. Packed-series deduplication and scan
accounting remain in the shared visitor. Hashing and persisted formats do not
change.

Retirement still protects folded collisions with logical locators, rejects
foreign-log locations, and removes a proposed location only if the currently
registered allocation is the same one. Durable index publication and compaction
fences are unchanged.

Run the focused checks from the repository root:

```bash
cargo test -p temporalstore-rust --lib --features alloc-probe \
  retention_work_tests -- --test-threads=1 --nocapture
cargo test -p temporalstore-rust --lib physical_wal_ -- --test-threads=1
```

The allocation check measures cumulative Rust allocator requests for 8,192
distinct string blocks. It compares identical retained sets against the previous
materialized algorithm. One debug build with `alloc-probe` requested 1,753,308
bytes in 28 allocation calls for the materialized scan and 295,212 bytes in 15
calls for the borrowed scan, about 83% fewer requested bytes. These are cumulative
requests, including growth, not live allocations. It is not a peak-RSS measurement or an endpoint latency
measurement. The scan remains proportional to live model blocks, and its final
set remains proportional to distinct retained folded identifiers. This change
does not bound the duration of the enclosing shard lock or establish large-corpus
Mem0 API performance acceptance.

A local storage-only diagnostic acknowledged all 16,384 records but still logged
a single metadata request at 815,054 microseconds before debugger attachment.
The captured maintenance stack was compressing the complete index during
publication. The run used a mock provider with extraction disabled and was
interrupted after stack capture; there is no complete endpoint report. Other
source files changed during the shared build, so this is not an isolated
before/after measurement. Real-model ingestion, extraction and retrieval
performance remain separate acceptance work.

The logical-object retention scan before the same publication now borrows the
model visitor too. It retains an object if any address lies outside the newly
durable slab, including both synthetic WAL address forms. The scan no longer
builds an owned block list before computing that object set. All model kinds,
mixed-slab objects and scan accounting remain covered; rollback and durability
ordering are unchanged.

For one 8,192-field hash object, a debug allocation probe requested 925,864 bytes
in 1,381 calls for the materialized scan and 401,576 bytes in 1,369 calls for the
borrowed scan. The 524,288-byte saving removes the owned list. Remaining visitor
deduplication still allocates; this is not an allocation-free or bounded-time
scan. Run its checks with:

```bash
cargo test -p temporalstore-rust --lib --features alloc-probe \
  compaction_retention_tests -- --test-threads=1 --nocapture
```

With both borrowed scans, another 16,384-record storage diagnostic acknowledged
all records but logged metadata requests at 813,371 microseconds (single) and
407,703 microseconds (batch) before debugger attachment. That capture showed a
request scanning set members in `any_element_other_than_at`, with maintenance
asleep. It does not establish which operation caused the earlier slow requests.
Rust-source fingerprints were unchanged during this test build and serving
build, but both used the shared development tree, not a clean public commit.
The diagnostic was interrupted after capture, so there is still no complete
endpoint or real-model acceptance report.
