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
| **Load stamped on every phase; `--max-load` marks the run PROVISIONAL.** | A p99 that measures a contended box rather than a store. This matters here: the host is shared, and figures taken while other work is building are not comparable with figures taken idle. |
| **Teardown checked against the shard's record count.** | A `teardown` that deleted nothing looking identical to a working one — and leaving every later point in a sweep reading a larger store. |
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

* **Requests are base64.** `HashMultiSet.entries` carries a bytes codec, so
  values go out base64 — a flat 1.33x on the payload.
* **Responses are decimal arrays.** `CommandResponse::Values`,
  `CommandResponse::Members` and `FeaturePoint.value` carry no such codec, so a
  byte comes back as the characters `104,`. For protobuf-serialized feature
  values that is roughly 3.8x the payload, so a read's wire cost is about 4x
  what it returns. The client decodes both shapes, so a node that gains the
  codec later needs no change.

## Reproducing

```bash
# a data node, a Valkey, a PostgreSQL
TS_SERVER_BIND_ADDR=127.0.0.1:17502 TS_SHARD_ID=1 ./target/release/server &
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

**The read-latency half of this sweep is incomplete, and the reason is a defect
this run found.** Only the 5,000-entity point produced usable read figures:

| entities | control drift | outcome |
|---:|---:|---|
| 1,000 | 27.5% | **VOID** — control exceeded the 25% tolerance, so no cross-store comparison from it is usable |
| 5,000 | 9.8% | clean; TemporalStore async warm p50 417 us / p99 567 us against Valkey 491 us / 673 us |
| 20,000 | — | **run refused**: the write reported success and the shard grew to 20,001 records, but entity 0 read back nothing |
| 50,000 | — | **run refused**, same way, at 50,001 records |

What the refused points did yield is the write path's shape, and it is clean
enough to name. With `track_keys_for_teardown` on, every written key is added
to a single per-view set:

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

**`track_keys_for_teardown` is therefore not usable above a few thousand
entities per view**, and that is now written where the option is documented. It
remains off by default. The entity-count read sweep needs re-running with it off
and a different means of bounding the shard between points; until then the read
figures above stand only at 5,000 entities and the question of how read latency
moves with entity count is **not answered here**.

The readback failure at 20,000 and 50,000 is **now attributed** — see "A
correctness defect that disqualifies one option" below. Those runs had
`track_keys_for_teardown` on against a shard configured for async commit, and
that pair loses rows. It took four probes to get there, and the first three
were worthless for a specific reason worth recording: each of them left
`async_storage` at the node's default of `false` while the failing arm had
forced it to `true`, so all three varied the wrong dimension and "not
reproduced" meant only that they had not tested the configuration that failed.


## Measured: serving QPS on the default write path, and what binds it

**Conditions.** Release build, a freshly created shard asserted at
`total_records = 0`, 5,000 entities, hot set 512, batch width 10, 1,200 read
calls per cell (12,000 rows), `track_keys_for_teardown` **off** — the default,
and the only safe setting under async commit (see below). Every cell's rows were
read back and byte-compared before any clock started, and every cell served
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

### The response-encoding change, specified and handed over

This section is a **specification, not an implementation** — the codec is owned
elsewhere, and response encoding is a client contract on a public repository.

Measured, at batch 10 and 1,035 B/row: the response is **38,421 B to deliver
10,490 B of feature values, 3.66x**. Giving `CommandResponse::Values` the bytes
codec that `HashMultiSet.entries` already carries on the request side would
send those values as 13,987 B of base64, making the response about **15,478 B —
60% fewer response bytes**. At 20 B/row the same change saves about **15%**,
because there the envelope rather than the payload is most of the response.

The latency expected from it is bounded by the +604.9 us the payload adds over
the floor, not by the byte ratio, so it should be justified as bandwidth and
tail relief under load rather than as p50.

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

## A correctness defect that disqualifies one option, measured

`track_keys_for_teardown` combined with `async_storage=true` **loses rows.**
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

The default serving path is unaffected: the flag is off by default, and async
commit with it off lost nothing in any run. Every QPS figure above was taken
with it off, which is also what a deployment runs. The option's own
documentation now carries this table, and the earlier entity-count sweep's
refusals at 20,000 and 50,000 entities are explained by it — those runs had the
flag on.

## A note on the shard a read is answered from

A read is answered out of a shard of some size, so a sweep that lets each point
leave its rows behind is not varying one dimension — it is varying entity count
*and* store size together. Measured on a first attempt: the shard went from
40,000 to 80,000 hash records across one sweep, because the command surface has
no key-space scan and `teardown` cannot enumerate what to delete unless
something recorded it. The sweep below therefore runs with
`track_keys_for_teardown` on and starts from an empty shard, and the record
count before and after each point is printed with it.

That flag's cost is on the **write** path only — one `set_add` per row — so the
write timings here are not comparable with a deployment that leaves it off. The
read columns are untouched by it.
