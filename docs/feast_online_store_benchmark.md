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

<!-- MEASURED-TABLES -->

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
