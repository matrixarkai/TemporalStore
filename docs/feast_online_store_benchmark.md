<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# Feature serving through Feast: what each online store measured

`integrations/feast/benchmarks/bench_feature_serving.py` drives several Feast
online stores through **Feast itself** and reports what each one did. The only
thing that differs between two rows of a table here is the `online_store` block
in the repo config; the entity keys, the feature values, the call sequence and
the client are the same code in every row.

This page is the method and the measured tables. Read the method first: most of
it is about figures the harness **refuses** to print, and those refusals are
why the numbers below are worth anything.

## What the harness refuses to report

A benchmark that did not move data measures nothing. Every one of these is a
check that fails the run rather than a warning in the output.

| Check | What it prevents |
| --- | --- |
| **Read back and compare bytes, before any clock starts.** Every sampled entity is read through the provider and each feature's serialized `ValueProto` is compared with what was written. | A store that accepted writes and holds nothing. A success code is not evidence the payload arrived. |
| **Assert the denominator.** The sampled row count, the cold-pass row count and each warm pass's row count are all compared with what was requested; a short answer fails the run. | Dividing into a beautiful number over a population of zero. A pass over an empty fixture proves only that nothing happened. |
| **Matched payloads across arms.** The per-row payload width is measured and asserted equal across every arm before any arm is compared. | A latency ratio between two stores holding different bytes. |
| **A control that the measurement cannot influence.** The SQLite arm runs first and again last. SQLite is untouched by anything the other arms do, so if its two p50s disagree by more than `--control-tolerance` (default 25%), the run is marked **VOID** and no cross-store comparison from it is usable. | Attributing a host's movement, or the harness's own drift, to a store. |
| **One control check per payload group.** | A guard that filters its own population down to one group and is then disarmed by adding a second. An earlier version of this block compared only the first payload's two SQLite runs and broke out of the loop, so a second payload's control could drift arbitrarily and the run still reported itself usable. |
| **Load stamped on every phase; `--max-io-pressure` marks the run PROVISIONAL.** | A p99 that measures a contended box rather than a store. This matters here: the host is shared, and figures taken while other work is building are not comparable with figures taken idle. |
| **Exact fixture cleanup checked against the shard's record count.** The command-surface arms delete the known entity keys after measurement. | A cleanup that deleted nothing looking identical to a working one — and leaving every later point in a sweep reading a larger store. |
| **Warm and cold are separate columns, never averaged.** | A single number that is neither. |
| **A fresh requested-feature list per read.** | Feast 0.67.0's Redis online store appends the timestamp key to the list the caller passed. Reusing one list grows the requested field set by one entry per read, so that arm would answer a longer and longer `HMGET` while every other arm answered a fixed one — the measurement quietly becoming a comparison of different work. It surfaced as an error thousands of reads later, which is the lucky outcome. |

Two further rules applied when reading these tables:

* **Every figure carries its profile.** The TemporalStore node is a **release**
  build. A figure without its profile is not reproducible.
* **No ratio is quoted across a summing aggregator.** Where two configurations
  are compared below, the comparison is computed per case from that case's own
  p50 or p99 and reported as a distribution, never as a mean of ratios.

## The configurations

Each row names exactly what produced it.

| arm | store | durability as configured | transport |
| --- | --- | --- | --- |
| `sqlite` / `sqlite_control` | SQLite, Feast's built-in | file on the run directory, default rollback journal | in-process, no network |
| `redis` | Valkey 8.1.10 (`valkey/valkey:8-alpine`) | **no persistence**: `--save ''`, `appendonly no` | RESP, pipelined, loopback |
| `postgres` | PostgreSQL 16.15 (`postgres:16-alpine`) | `synchronous_commit=on`, `fsync=on`, `PGDATA` on a 2 GB tmpfs | libpq, loopback |
| `ts_async` | TemporalStore, this repo | async commit (`async_storage=true`) | HTTP `POST /batch_execute`, one round trip per call |
| `ts_sync` | TemporalStore, this repo | durable commit (`async_storage=false`) | HTTP `POST /batch_execute`, one round trip per call |

Every setting in that table was read back from the running server rather than
taken from the config that was sent: `async_storage` is verified by a
`GET /get_config/{shard}` after the write, and the run aborts if it reads back
as anything other than what was asked for. The Valkey persistence settings and
the PostgreSQL commit settings were likewise confirmed with `CONFIG GET` and
`pg_settings`.

TemporalStore appears twice on purpose. A store answering from memory and a
store committing durably per write are not the same system, so durability is a
column rather than a footnote.

### Contention controls — dimensions the harness could not vary

These are read and reported rather than left out, because they bound what the
tables can be used for:

* **Client concurrency is 1.** Feast's serving path is synchronous and the
  harness drives it from one thread. Every figure here is a single-client
  latency; none of them is a saturation throughput.
* **Valkey runs with `io_threads_active:0`** — a single I/O thread. Its figures
  are a single-threaded server's.
* **SQLite is in-process.** It pays no network cost at all, which is why it is
  the control rather than a competitor: it is the arm whose cost is independent
  of everything the others do.
* **The host is shared**, 16 cores, with other builds running at times. The
  load at the start and end of every phase is recorded, and any run whose peak
  1-minute load exceeded `--max-load` is labelled PROVISIONAL below.

### What the wire costs on this path, which is transport and not engine

Stated here so nobody reads a bandwidth number off the command surface without
it:

* **Binary requests preserve byte strings.** For 4,096 commands carrying
  12,296-byte values, MessagePack produced 50,605,338 bytes versus 67,460,469
  bytes for JSON, a 24.99% reduction. Median client encoding time for that
  exact command set was 55.41 ms versus 273.01 ms.
* **Binary responses preserve byte strings.** A ten-row response carrying
  12 KiB per row measured 123,551 bytes versus 164,811 bytes through JSON
  base64, a 25.03% reduction. Older nodes remain usable through the JSON
  compatibility routes.

### Binary read/write scale result

Release build, 4,096 entities, one 12,296-byte sequence per entity, batch 10,
500 measured read calls, and eight concurrent readers. All 256 sampled values
matched exactly in every arm, and teardown removed all 4,096 records in both
TemporalStore modes. The SQLite control moved 2.1% from the first run to the
last, inside the 25% rejection threshold.

The server used isolated tmpfs cache, block, index, and Raft directories.
Storage maintenance was disabled for this run so it measures the command API
and codec rather than a maintenance cycle; these are transport-serving
figures, not a maintenance-under-load claim.

| mode | write rows/s | warm rows/s, batch 10 | concurrent rows/s, 8 workers | concurrent p50 | concurrent p99 |
|---|---:|---:|---:|---:|---:|
| async commit | 4,985 | 12,289 | 4,585 | 14,500 us | 36,781 us |
| durable commit | 3,437 | 6,204 | 9,344 | 8,086 us | 15,885 us |

One exact 4,096-command encoding probe, using the same 12,296-byte payload,
produced 50,605,338 MessagePack request bytes in a median 55.41 ms versus
67,460,469 JSON bytes in 273.01 ms: 24.99% fewer bytes and 4.93x faster client
encoding for that command set. This isolates the request-encoding change from
server and host scheduling.

## Reproducing

For an isolated large-ingest sample, start with an empty shard and run one arm
and one payload. `--skip-teardown` keeps the known fixture keys after sampled
value verification, avoiding a potentially long deletion phase outside the
timed writes. The report names the write-batch size and marks that teardown
was skipped. Remove the isolated store directory before the next sample.

```bash
PYTHONPATH=integrations/feast python integrations/feast/benchmarks/bench_feature_serving.py \
  --arms ts_sync --payloads large --entities 50000 --write-batch 500 \
  --hot 512 --reads 1 --cold-reads 1 --skip-teardown \
  --temporalstore 127.0.0.1:17502
```

This invocation has no SQLite control and cannot support a claim against
another store. The controlled multi-arm run below retains its default teardown
and verification behavior. Record the node build, configuration, load, and
block/index/WAL bytes along with throughput when comparing storage settings.

```bash
# a data node with every persistent directory isolated for this run
BENCH_NODE=/dev/shm/feast-command-benchmark-node
rm -rf "$BENCH_NODE"
mkdir -p "$BENCH_NODE"/{cache,blocks,indexes,raft-wal}
MATRIXARK_STORAGE_MANAGER_INTERVAL_MS=0 \
TS_CACHE_DIR="$BENCH_NODE/cache" \
TS_BLOCK_STORE_DIR="$BENCH_NODE/blocks" \
TS_INDEX_DIR="$BENCH_NODE/indexes" \
TS_RAFT_WAL_DIR="$BENCH_NODE/raft-wal" \
TS_SERVER_BIND_ADDR=127.0.0.1:17502 TS_SHARD_ID=1 \
  ./target/release/server &
docker run -d --name bench-redis -p 127.0.0.1:6380:6379 valkey/valkey:8-alpine \
  valkey-server --save '' --appendonly no
docker run -d --name bench-pg -p 127.0.0.1:55432:5432 \
  -e POSTGRES_USER=feast -e POSTGRES_PASSWORD=feast -e POSTGRES_DB=feast \
  -e PGDATA=/pgtmp/data --tmpfs /pgtmp:size=2g postgres:16-alpine

pip install 'feast==0.67.0' redis 'psycopg[binary,pool]'
cd integrations/feast
PYTHONPATH="$PWD" python benchmarks/bench_feature_serving.py \
  --arms sqlite,redis,postgres,ts_async,ts_sync,sqlite_control \
  --payloads small --entities 5000 --hot 512 --reads 1500 \
  --temporalstore 127.0.0.1:17502
```

The harness exits non-zero when a run is PROVISIONAL or VOID, so it can gate.

For long list-valued features, use `--payloads sequence`. That profile writes
one 4,096-element Feast `Array(Int64)` per entity, verifies serialized values
after materialization, and reports both write rows/s and concurrent read rows/s
when `--concurrency` exceeds one. The command-surface arms use bounded HTTP
batches and delete the fixture's known keys after timing; they do not enable
key tracking on an async-commit shard.

The separate aggregate workload exercises the Feast command adapter's
`write_feature_history` path. It streams integer event batches, verifies the
latest served value plus full and tail-window sums and raw-history counts for
every entity, measures concurrent native aggregate queries, and removes all
known hash and history keys afterward:

```bash
cd integrations/feast
PYTHONPATH="$PWD" python benchmarks/bench_aggregate_serving.py \
  --host 127.0.0.1 --port 17502 --entities 256 --events 64 \
  --reads 2000 --concurrency 4
```

The resulting QPS is a single-node measurement, not a cross-store comparison;
the benchmark marks the run `void` if the same-path control p50 drifts beyond
`--control-tolerance` (25% by default). Repeat under a controlled host load
before using the number for a capacity decision.

### Measured aggregate history serving

Two consecutive valid runs used a release standalone data node with durable
commit (`async_storage=false`), its default 16 MiB cache, 1,024 entities, 64
events per entity, write batches of 256 rows, 4,000 aggregate reads, and eight
read workers. Every run verified latest values, full-window sums and counts,
and tail-window sums before timing, then returned the shard to zero records.

| run | history events/s | aggregate queries/s | p50 | p99 | control drift |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 14,965 | 1,482 | 4,848 us | 12,737 us | 0.28% |
| 2 | 15,260 | 1,552 | 4,566 us | 13,106 us | 15.80% |

These are same-store, local single-node measurements. They do not establish
multi-node capacity or a comparison with another online store.

The aggregate harness now also measures the bounded batch API at 64 queries
per call. It discards one cache-fill control pass, runs the batch phase three
times, reports the median, and marks the run void when any repeat differs from
that median by more than 25%. One accepted run on the same 1,024 × 64 workload
measured **4,207 aggregate queries/s**, with repeats 4,207 / 4,714 / 4,187
(12.0% maximum median drift). Batch-call p50 was 13,327 us and p99 16,906 us;
the eight-worker one-query path in that run served 1,914 queries/s. Every
latest value, full and tail sum, and history count matched, and cleanup returned
the shard to zero records. Other attempts whose controls or batch repeats moved
beyond tolerance were rejected rather than averaged into this result.

## Measured: single-client read latency

**Conditions for both tables below**, identical except the payload: 5,000
entities, hot set 512, 1,500 reads per cell, batch width 10 where stated, one
client thread, 16-core shared host at 1-minute load 3 with no compiler running,
TemporalStore a **release** build on a **freshly created, empty shard**
(`total_records = 0` asserted before the run). Each arm's rows were read back
and compared byte for byte against what was written before any clock started;
each TemporalStore arm's `teardown` was confirmed to have removed exactly
`entities + 1` records, returning the shard to 0.

Both runs exited 0: the control held (drift 17.3% and 13.0%, tolerance 25%) and
peak load stayed under `--max-load`.

### 20 B of feature payload per row (ten int64 features)

| store | durability | cold p50 | cold p99 | warm p50 (b=1) | warm p99 (b=1) | warm p50 (b=10) | warm p99 (b=10) | warm rows/s (b=10) | serving p50 (b=10) | serving p99 (b=10) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SQLite | rollback journal, in-process | 72 us | 162 us | 73 us | 213 us | 452 us | 866 us | 20,053 | 1,025 us | 2,653 us |
| Valkey 8.1.10 | no persistence | 479 us | 1,041 us | 477 us | 637 us | 925 us | 1,458 us | 10,486 | 1,830 us | 2,926 us |
| PostgreSQL 16.15 | `synchronous_commit=on`, tmpfs | 703 us | 1,206 us | 825 us | 1,806 us | 1,192 us | 2,611 us | 7,076 | 2,437 us | 4,277 us |
| **TemporalStore** | **async commit** | **396 us** | **532 us** | **355 us** | **484 us** | 970 us | 1,387 us | 10,130 | 1,771 us | 2,731 us |
| **TemporalStore** | **durable commit** | 656 us | 948 us | 410 us | 558 us | 933 us | 1,610 us | 10,204 | 2,242 us | 3,444 us |
| SQLite (control re-run) | rollback journal, in-process | 83 us | 186 us | 86 us | 198 us | 433 us | 919 us | 21,270 | 1,060 us | 2,237 us |

At this payload the async-commit configuration answered a single-entity read
faster than the Valkey configuration in every percentile measured, computed per
case rather than averaged: cold p50 17% lower (396 vs 479 us), cold p99 49%
lower (532 vs 1,041 us), warm p50 26% lower (355 vs 477 us), warm p99 24% lower
(484 vs 637 us). At batch width 10 the two are level — warm p50 5% higher (970
vs 925 us), warm p99 5% lower (1,387 vs 1,458 us), throughput 3% lower. The
durable-commit configuration costs 37% on the cold p50 (656 vs 396 us) and 15%
on the warm p50 (410 vs 355 us) against its own async configuration, which is
the price of the fsync and is why durability is a column here.

### 1,035 B of feature payload per row (four int64 features and a 1 KiB blob)

| store | durability | cold p50 | cold p99 | warm p50 (b=1) | warm p99 (b=1) | warm p50 (b=10) | warm p99 (b=10) | warm rows/s (b=10) | serving p50 (b=10) | serving p99 (b=10) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SQLite | rollback journal, in-process | 82 us | 186 us | 63 us | 191 us | 354 us | 737 us | 25,412 | 813 us | 1,658 us |
| Valkey 8.1.10 | no persistence | 472 us | 616 us | 455 us | 619 us | **861 us** | **1,223 us** | **11,381** | **1,509 us** | **2,728 us** |
| PostgreSQL 16.15 | `synchronous_commit=on`, tmpfs | 705 us | 1,118 us | 715 us | 1,817 us | 1,186 us | 2,174 us | 8,006 | 2,291 us | 5,860 us |
| TemporalStore | async commit | 479 us | 716 us | 445 us | 607 us | 1,614 us | 2,700 us | 5,923 | 2,453 us | 6,127 us |
| TemporalStore | durable commit | 615 us | 948 us | 470 us | 690 us | 1,702 us | 4,287 us | 5,366 | 2,331 us | 3,520 us |
| SQLite (control re-run) | rollback journal, in-process | 59 us | 176 us | 55 us | 140 us | 286 us | 557 us | 31,518 | 887 us | 1,573 us |

**This result is unfavourable and is reported as measured.** At a 1 KiB payload
and batch width 10, the async-commit configuration is behind the Valkey one by
88% on warm p50 (1,614 vs 861 us), 121% on warm p99 (2,700 vs 1,223 us) and 48%
on throughput (5,923 vs 11,381 rows/s). At batch width 1 the two remain level
(warm p50 445 vs 455 us), and the 20 B table above shows no such spread at
either width — so the cost appears only where the payload is large **and** many
rows are fetched per call.

The wire format accounts for it arithmetically rather than by supposition.
A response payload on this surface is a decimal array, about 3.8 characters per
byte, so a batch of 10 rows × 11 fields × ~1 KiB of values is roughly 11 KB of
feature bytes arriving as ~42 KB of JSON text to be tokenized and converted
element by element. At 20 B per row that text is negligible and the spread does
not appear; at 1 KiB it dominates the call. This is the **transport**, not the
engine, and it is the measured case for giving `CommandResponse::Values` the
same bytes codec that `HashMultiSet.entries` already carries on the request
side.

## Measured: scalability over entity count

One dimension varied — entity count — with payload, hot set, read count, batch
width and client concurrency all fixed, each point starting from an empty shard.

The original read-latency half of this sweep was incomplete because the former
tracking layout failed before the larger points could be measured. Only the
5,000-entity point produced usable read figures at that time:

| entities | control drift | outcome |
|---:|---:|---|
| 1,000 | 27.5% | **VOID** — control exceeded the 25% tolerance, so no cross-store comparison from it is usable |
| 5,000 | 9.8% | clean; TemporalStore async warm p50 417 us / p99 567 us against Valkey 491 us / 673 us |
| 20,000 | — | **run refused**: the write reported success and the shard grew to 20,001 records, but entity 0 read back nothing |
| 50,000 | — | **run refused**, same way, at 50,001 records |

What the refused points did yield was the former write path's shape. Every
written key was added to one per-view set:

| entities | TemporalStore write | vs 5,000 | Valkey write | SQLite write |
|---:|---:|---:|---:|---:|
| 5,000 | 3.05 s | 1.0x | 0.40 s | 0.50 s |
| 20,000 | 39.94 s | 13.1x | 1.29 s | 3.24 s |
| 50,000 | 463.67 s | 152.0x | 3.43 s | 9.11 s |

Ten times the rows for about a hundred and fifty times the work: that is
quadratic, and the mechanism is visible in the shard's own counters — the
tracking set is `set_records: 1`, one key, and every row's `set_add` is a write
to it. A container write that costs its container's size makes N inserts cost
O(N²). The two comparison arms are linear in the same test
(Valkey 8.6x and SQLite 18.2x for 10x the rows), so this is a property of this
write path and not of the host.

That single-set layout has been replaced by 1,024 deterministic tracking sets.
The shard count is fixed so changing configuration cannot strand tracked rows,
and teardown still reads the former unsharded key for rolling upgrades.

The focused benchmark at
`integrations/feast/benchmarks/bench_teardown_tracking.py` drives the Feast
command provider against a release node on a fresh tmpfs-backed shard. Batch
width is 500, every value is read back and compared, each point starts from
empty directories, and warm serving uses a fixed 512-entity hot set. Each warm
point runs three times and is rejected when throughput differs from its median
by more than 25%:

| entities | write | write rows/s | read rows/s | largest tracking set | missing or mismatched |
|---:|---:|---:|---:|---:|---:|
| 5,000 | 0.94 s | 5,329 | 8,964 | 11 | 0 |
| 20,000 | 5.28 s | 3,785 | 8,009 | 29 | 0 |
| 50,000 | 7.80 s | 6,412 | 11,061 | 62 | 0 |

Ten times the rows now takes 8.3 times the elapsed write time instead of the
historical 152 times. At 50,000 rows all 1,024 tracking sets are occupied,
average 48.83 members, so the largest container is close to the expected
`N/1024` rather than holding all 50,000 members. The flag remains off by
default because it adds one write per row, not because its container size is
unbounded.

The repeat-gated warm read sweep closes the earlier entity-count question.
Every point uses batch 10, eight concurrent readers, 500 calls per repeat and
the same 512 hot entities:

| entities | warm rows/s | call p50 | call p99 | repeat throughput | max median drift |
|---:|---:|---:|---:|---|---:|
| 5,000 | 21,070 | 3,488 us | 8,562 us | 20,610 / 21,070 / 21,175 | 2.18% |
| 20,000 | 10,428 | 6,852 us | 17,838 us | 10,428 / 10,463 / 10,364 | 0.62% |
| 50,000 | 10,316 | 6,892 us | 20,060 us | 10,316 / 10,606 / 10,096 | 2.81% |

There is a clear serving step between 5,000 and 20,000 resident rows, followed
by a plateau: 20,000 and 50,000 have effectively the same throughput and p50.
The measurement establishes the shape but does not assign a cause; distinguishing
an index-residency boundary from a cache-tier boundary needs a capacity sweep.

The benchmark's `--verify-teardown` arm also ran 1,000 rows through the full
cleanup path in 0.184 s. A post-cleanup read found zero remaining feature rows
and all 1,024 tracking sets contained zero members.

The historical readback failure at 20,000 and 50,000 is attributed below.
Those runs had
`track_keys_for_teardown` on against a shard configured for async commit, and
that pair lost rows before the WAL container lookup change. It took four probes
to get there, and the first three
were worthless for a specific reason worth recording: each of them left
`async_storage` at the node's default of `false` while the failing arm had
forced it to `true`, so all three varied the wrong dimension and "not
reproduced" meant only that they had not tested the configuration that failed.


## Measured: serving QPS on the default write path, and what binds it

**Conditions.** Release build, a freshly created shard asserted at
`total_records = 0`, 5,000 entities, hot set 512, batch width 10, 1,200 read
calls per cell (12,000 rows), `track_keys_for_teardown` **off** — the default,
so tracking work is outside these serving figures. Every cell's rows were read
back and byte-compared before any clock started, and every cell served
12,000/12,000. 16-core shared host, 1-minute load 6.1 falling to 4.4, one
compiler running. Concurrency is coroutines over `online_read_async`.

### Reads, 20 B of payload per row (ten int64 features)

| concurrency | p50 | p99 | calls/s | rows/s |
|---:|---:|---:|---:|---:|
| 1 | 1,908 us | 7,146 us | 457 | 4,566 |
| 2 | 2,230 us | 4,060 us | 799 | 7,990 |
| **4** | 4,096 us | 8,441 us | **892** | **8,919** |
| 8 | 9,274 us | 21,118 us | 741 | 7,408 |
| 16 | 17,464 us | 103,411 us | 787 | 7,869 |

(`async_storage=true`. The same sweep under durable commit peaks one step
earlier — 7,964 rows/s at concurrency 2 — and its tail collapses at 16, where
p99 reached **1,222,073 us**, 1.2 seconds.)

**Throughput peaks at concurrency 4 and does not improve after it**, while p99
grows 12x between 4 and 16. The ceiling for this payload is about **8,900
rows/s / 890 calls/s from a single client process**, and past that point extra
concurrency buys tail latency rather than throughput.

### Reads, 1,035 B of payload per row

Flatter and roughly half the throughput: 4,467 rows/s at concurrency 1, peaking
at **5,147 rows/s** (concurrency 8, async commit) and 5,168 rows/s (concurrency
4, durable commit). Wide rows cost about 1.7x per row against the narrow case
at the peak.

### What actually binds a read, priced apart

Same batch read, same node, three payload states — absent keys transport no
payload at all, so that cell is the envelope-and-engine floor:

| cell | field slots | response | feature bytes | p50 | vs floor |
|---|---:|---:|---:|---:|---:|
| absent keys (**floor**) | 110 (all null) | 1,491 B | 0 | **1,026.6 us** | — |
| 20 B/row | 110 | 2,336 B | 376 B | 1,758.4 us | +731.8 us |
| 1,035 B/row | 60 | 38,421 B | 10,490 B | 1,631.5 us | +604.9 us |

**Two things here were not what the earlier payload comparison suggested.**

First, **the 1 KiB cell carries 36,930 more response bytes than the floor and
costs LESS added latency than the 20 B cell does** (+605 us against +732 us).
So at batch 10 the response encoding is a bandwidth cost, not the dominant
latency cost.

Second, the ordering follows the number of **field slots** fetched, not the
bytes in them: 110 slots holding 376 B in total cost more than 60 slots holding
10,490 B. Field count is a first-order term and payload width a second-order
one at these sizes. A view with many narrow features is therefore the expensive
shape, which is the opposite of the intuition a byte-count model gives.

And the floor itself is the largest single component: **1,026.6 us to answer a
ten-command batch that returns nothing at all**, about 63% of the wide-row
read's p50 and 58% of the narrow one's. Reducing per-call and per-field
overhead is worth more than any payload encoding at this batch width.

### Binary batch transport

The native command integration now uses MessagePack in both directions for
bounded read and compact-write batches. Byte-bearing command fields remain
byte strings rather than expanding to base64 on the request side, and response
values remain byte strings on the response side. JSON routes remain a
one-time-detected compatibility path for older nodes.

The measured reductions above describe wire and client-encoding work. They do
not by themselves establish a server-latency multiplier; latency and QPS are
reported separately from full Feast serving runs.

### Writes

Reported, but **not quotable as a durability ratio**: one measurement per cell,
and the ordering is inconsistent between payloads, which means the differences
are inside the noise of a contended host rather than a property of the commit
mode.

| payload | async commit | durable commit |
|---|---:|---:|
| 20 B/row | 3,695 rows/s | 2,030 rows/s |
| 1,035 B/row | 1,703 rows/s | 3,760 rows/s |

Durable commit is faster in one row and slower in the other, which cannot both
be true of the mechanism. A usable write figure needs repeated runs on a quiet
host; what these do establish is the **order of magnitude: a few thousand rows
per second per client at a batch width of 500**, with the earlier latency table
showing durable commit costing 37% on the cold read p50.

## Async commit with exact teardown tracking

The original measurement below found a data-loss defect when one shared set
was updated beside asynchronously committed feature hashes. Container blocks
under cache pressure could not always locate the WAL record carrying their
bytes. The read path now falls back from an element-specific registration to
the owning container record and selects the requested block within that record.

The same standalone workload was rerun after that change with a release build,
a freshly created empty shard, 20,000 feature-shaped rows, batch width 500,
`async_storage=true`, and one shared tracking set. It retained **20,000 of
20,000 rows** and reported `LOST=0/20000`. Exact teardown tracking and async
commit are therefore supported together. The option remains off by default
because the shared-set write scales poorly, as measured above.

### Original failure measurement

Before the WAL container lookup change, `track_keys_for_teardown` combined
with `async_storage=true` lost rows.
Release build, a freshly created shard asserted empty, 20,000 entities at batch
width 500:

* **17,500 of 20,000 rows read back empty.** Not late — empty.
* The shard reported `total_records = 20001`, so **a record count is not
  evidence a record has content**: `HashGetAll` on a lost row's key answered
  with **zero fields**.
* A 20-second pause did not bring them back, so this is loss and not a
  visibility delay.
* Only the last 2,500 writes — the final five batches — survived, which is the
  shape of older dirty objects being dropped before they were persisted.

It is an **interaction**, which is why it survived earlier probing: neither
half does it alone, and three separate probes that varied only one of them
cleared both.

| `async_storage` | `track_keys_for_teardown` | rows lost of 20,000 |
|---|---|---:|
| false | true | 0 |
| true | false | 0 |
| **true** | **true** | **17,500** |

Every QPS figure above was taken with tracking off. The historical table is
kept here because it defines the workload used by the regression script and
shows why a smaller smoke test was insufficient.

### Standalone regression workload

`tools/repro_async_commit_row_loss.py` drives the data node's own command
surface with the standard library only — no Feast, no client library, no
third-party package. Before the fix it reproduced the failure:

```
async_storage=True  hot_set=True   N=20000  (fresh empty shard)
  LOST = 16,000/20,000     total_records = 20001
  HashGetAll(lost key) -> 0 fields. The row is counted and holds nothing.
  after 20s: STILL LOST
```

That result was the same magnitude and shape as the provider failure: early
batches disappeared and the last rows survived. The current release-build run
of the identical script reports zero loss.

**One property of the reproduction is worth stating because it cost two runs:
the failing workload has to be the FIRST thing on the shard.** Run third, after
two other cells had each left 20,000 rows, the identical cell lost nothing.
Object footprint matters for the same reason: a first version of the script used
short keys and one-byte values and did not reproduce, and only matched the
provider once the keys were the width a serialized entity key produces (62 hex
characters) and each row carried eleven field slots. The loss tracks what the
shard is holding against its cache budget, so a smaller row is a different
experiment rather than a simpler one. The script's defaults encode both.

A caution for anyone extending that script: its first version reported
20,000 of 20,000 lost on a shard that was holding every row, because the writer
had been moved to one set of field names and the reader left on another. The
`HashGetAll` probe is what exposed it — it printed eleven fields present on a
key the script had just called lost — and the field list is now derived once and
used by both sides. A reader that builds its own copy of the field list is the
bug.

## Two operational findings that bear on readiness, not on QPS

### A liveness probe that reports healthy while every data operation is blocked

**This replaces an earlier claim in this document that `/server/info` walks
every object under the shard read lock. That claim was wrong, and the
measurement is below.** It is corrected rather than amended because a wrong
attribution left standing beside a correction is still one someone will quote.

The observation was real: at roughly 100,000 records `GET /server/info` exceeded
30 s. What was wrong was the cause. The stall co-occurred with a `HashMultiGet`
timing out on the same node at the same moment — the node was **saturated by a
hot-key write storm**, and everything needing the shard lock queued behind it.
`/health` takes no shard lock and touches no shard state, so it kept answering
**200 in 1.0 ms** throughout.

That is the finding, and it is worse than a slow endpoint: an operator cannot
distinguish a saturated node from a healthy one, and an orchestrator watching
`/health` will neither restart it nor take it out of rotation. **The honest
statement is that the health endpoint reports process liveness, not shard
serviceability.**

**It does not live on the status path.** No change to `/server/info` or
`shard_stats` would move it; it lives wherever one key's writes are allowed to
monopolise the shard lock. That matters for whoever fixes it, so it is stated
rather than implied.

#### The measurement that refuted the original attribution

Release build, fresh shard, p50 over seven calls, key count and ref count varied
independently so a walk over either would show:

| shard | `/server/info` p50 | `/health` |
|---|---:|---:|
| empty | 1.8 ms | 1.0 ms |
| 20,000 keys / 20,000 refs | 1.8 ms | 1.1 ms |
| 22,000 keys / 40,000 refs | 1.7 ms | 0.9 ms |
| 42,000 keys / 60,000 refs | 2.7 ms | 1.4 ms |

Three code-reading diagnoses, each wrong, recorded so they are not re-derived:

1. **The `object_element_name_block_refs` fallback walk is unreachable.** The field
   is `Some(0)` from birth — the type's own hand-written `Default` states it and
   explains why — and nothing anywhere assigns `None`. Removing the
   `unwrap_or_else` would change no behaviour.
2. **The WAL full-file scan really was the dominant cost, and is already
   fixed.** `wal.rs` records it at **771,062 of 776,311 bytes, 99.3%**, of one
   `shard_stats` read, and `stats_without_rescan` exists for it and **is
   wired**, at `engine_persistence.rs:1352`.
3. **The thirteen per-map sums do not move the figure** at these sizes.

The transferable lesson: **a code comment that predicts a symptom is not
evidence that the symptom has that cause.** The comment in
`object_manager_stats` predicts write contention from walking under the read
lock, accurately — and was still the wrong attribution for what was observed.

### Teardown's cost is the delete command, not the batching or the set read

The previous round bounded teardown's batches at 512 commands and it still took
2,065 s for 20,000 keys, so the remaining cost was worth decomposing before
optimising anything. Measured on a release build against a fresh empty shard,
4,000 keys, batch 512:

| teardown step | time | per key | share |
|---|---:|---:|---:|
| `SetMembers` read (4,000 members, 260,089 B response) | 0.12 s | — | 0% |
| **the `CommonDelete` batches** | **30.01 s** | **7.50 ms** | **100%** |
| dropping the tracking set itself (one key, 4,000 members) | 0.02 s | — | 0% |
| *(for scale: writing the same 4,000 hashes)* | *1.54 s* | *0.39 ms* | — |

**A delete costs 19.5x a write of the same keys**, and neither of the two parts
that looked suspicious contributes anything measurable: the big `SetMembers`
response is 0% and dropping a 4,000-member set is 0%.

The per-batch profile says it is not flat per-key work either: **the first batch
took 7.95 s and the last 0.43 s**, an 18x spread across eight batches of the
same size. Something is paid once, early, rather than per key.

It also does not extrapolate: 7.50 ms/key at 4,000 keys against 103 ms/key
implied by the 2,065 s at 20,000 keys — 14x worse per key for 5x the keys. The
two runs differ in host load and in what the hot tracking set had done to the
shard, so the right reading is that per-key delete cost itself grows with the
shard, and a single-point measurement of it is not a rate. Anyone optimising
this should start at `CommonDelete`, not at the batching.

#### Answered: each delete batch does work proportional to the WHOLE shard

The measurement was the one written down here as open: **the same 2,000 deletes,
once on a shard holding only those 2,000 and once on a shard holding 20,000
filler keys as well, varying nothing else.** Release build, fresh shard each
time, batch 512, host load 3.4 with no compiler running.

| the shard holds | 2,000 deletes | per key | per-batch profile |
|---|---:|---:|---|
| just those 2,000 keys | 4.48 s | **2.24 ms** | 2.28 s then 0.18 s — front-loaded |
| those plus 20,000 filler | 138.34 s | **69.17 ms** | 37.20 s … 29.60 s, max 37.52 s — flat and high |

**31x the per-key cost for identical deletes, with only the shard's size
changed.** Delete cost scales with what the shard holds, not with the batch and
not with the number of keys being deleted.

**And the per-batch profile changes shape, which corrects the earlier reading
here.** On the small shard the cost is front-loaded — 2.28 s then 0.18 s — which
is what the 4,000-key run showed and what was described as "something is paid
once rather than per key". On the large shard it is **flat and high**: every
batch pays 30–37 s. So the front-loading was a small-shard artefact, and at
scale each batch pays the shard-size cost afresh. Over N/512 batches that is
work proportional to (keys deleted x shard size), which is why the totals looked
quadratic and why 7.50 ms/key at 4,000 keys did not extrapolate to the 103
ms/key implied at 20,000.

That narrows the search for whoever optimises it: **look for what a
`CommonDelete` batch touches that is proportional to the resident shard rather
than to the keys in the batch** — a scan, a rebuild, or a structure rewritten
once per batch. It is not the batching (already bounded at 512), not the
`SetMembers` read (0% of teardown), and not the tracking-set drop (0%).

## A note on the shard a read is answered from

A read is answered out of a shard of some size, so a sweep that lets each point
leave its rows behind is not varying one dimension — it is varying entity count
*and* store size together. Measured on a first attempt: the shard went from
40,000 to 80,000 hash records across one sweep, because the command surface has
no key-space scan and `teardown` cannot enumerate what to delete unless
something recorded it. The sweep below therefore runs with
`track_keys_for_teardown` on and starts from an empty shard, and the record
count before and after each point is printed with it.

That flag's cost is on the **write** path only — one `set_add` per row,
distributed over 1,024 sets — so the write timings here are not comparable
with a deployment that leaves it off. The read columns are untouched by it.
