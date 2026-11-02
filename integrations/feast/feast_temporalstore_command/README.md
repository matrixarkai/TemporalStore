# TemporalStore as a Feast online store — over the native command surface

A [Feast](https://github.com/feast-dev/feast) **online store** that talks to a
TemporalStore data node over its own command surface: `POST /batch_execute`
with `HashMultiSet` / `HashMultiGet` commands.

## Which integration point this is

Feast has three pluggable slots that are easy to conflate. This is the first
one:

| slot | what it does | this package |
| --- | --- | --- |
| **online store** | serves the latest feature values to `get_online_features` | **yes — this is it** |
| offline store | answers point-in-time-correct historical queries for training | no |
| materialization engine | moves rows from the offline store into the online store | no — the stock engines write **into** this store by calling `online_write_batch` |

So TemporalStore becomes a *materialization target* as a consequence of being
an online store, which is the normal arrangement; nothing here replaces
`feast materialize`.

## Install and configure

```bash
pip install feast
export PYTHONPATH=$PWD/integrations/feast
```

```yaml
# feature_store.yaml
project: my_project
provider: local
registry: data/registry.db
entity_key_serialization_version: 3
online_store:
  type: feast_temporalstore_command.TemporalStoreCommandOnlineStore
  host: 127.0.0.1
  port: 17102
  shard_id: 1
  skip_dedup: false          # see "Writes" below
  key_ttl_seconds: null      # see "Teardown" below
  write_feature_history: false
```

Bring a single-node TemporalStore up with
`docker compose -f docker-compose.single-node.yml up`, which serves the command
surface on port 17102.

## Why the command surface and not the RESP port

TemporalStore also has a Redis-protocol front end, which lets Feast's stock
Redis online store point at it. That path works, and it is a reasonable thing
to want. It costs three things this one does not:

1. **Three processes instead of one.** The RESP front end's internal client
   refreshes table topology before a write, which a bare data node does not
   serve, so it needs `metaserver` + `datanode` + `proxy` behind it.
2. **One HTTP round trip per Redis command.** `serve_redis_proxy` issues one
   `/execute` per command it decodes, so a pipelined `HMGET` over N entities
   becomes N sequential round trips. This package issues **one**
   `/batch_execute` for the whole entity list, whatever N is.
3. **A byte-encoding workaround.** Feast's Redis store names a hash field by
   the 4-byte little-endian murmur3 of `"{view}:{feature}"` — arbitrary bytes.
   A hash field on the command surface is a `String`, so this package uses the
   readable `"{view}:{feature}"` directly and never has to encode a digest at
   all.

## Layout

One TemporalStore hash per `(project, entity key)` — the same granularity
Feast's Redis store uses.

| part | value | note |
| --- | --- | --- |
| key | `"{hex(serialize_entity_key(ek))}:{project}"` | TemporalStore keys are UTF-8 `String` and a serialized entity key is binary, so it is hex-encoded: **2x on the key, nothing on the value** |
| field | `"{view}:{feature}"` | readable; Feast's Redis store stores a 4-byte digest here instead |
| field | `"_ts:{view}"` | the row's event timestamp, a serialized protobuf `Timestamp` |
| value | `ValueProto.SerializeToString()` | **byte-identical** to what Feast's Redis, SQLite and Postgres stores keep |

Values matching byte for byte is what makes a latency comparison against those
stores mean anything. Keys do not match — the hex encoding is a real difference
in stored key footprint, and it is written down here so nobody quotes a
footprint ratio without knowing.

## Reads

`online_read` over N entity keys is **one** `/batch_execute` carrying N
`HashMultiGet` commands. The answers are positional; a short answer is refused
rather than zipped, because a missing slot would shift every value onto the
wrong feature and read downstream as a stale feature rather than as an error.

An entity that was never written reads back as `(None, None)`, not as a dict of
empty values.

## Writes

`online_write_batch` is one `/batch_execute` of `HashMultiSet` commands, plus
one extra batched read first unless `skip_dedup` is set:

* **`skip_dedup: false` (default)** — read each row's stored `_ts` field, then
  write only the rows whose event time is newer. Two round trips for the whole
  call, not two per row. This is what stops an out-of-order materialization
  moving a feature backwards, and it also deduplicates *within* the call, so
  two rows sharing an entity key keep the newer one regardless of which was
  queued last.
* **`skip_dedup: true`** — one round trip. Right for an initial load or an
  append-only pipeline whose rows arrive in order.

### `write_feature_history`

Off by default. When on, every value is *also* appended to a feature series
keyed by `(view, entity, feature)` at its event timestamp, so the history
behind the served value stays queryable through `FeatureQuery` and
`FeatureAggQuery` — windowed aggregates over the same data the online store is
serving, which a plain key-value online store cannot offer.

Serving reads do not change: `online_read` always answers from the hash. That
is deliberate, and it is worth knowing why. `FeatureQuery` returns a window in
**ascending** timestamp order and truncates with `take(count)`, so asking it
for "the latest point" costs O(points in the window), not O(1). It is a history
read, not a latest-value read. Turning this flag on adds one `FeatureAppend`
per feature per row to the write path and leaves the read path alone.

## Teardown

TemporalStore's command surface has **no key-space scan** — there is no `SCAN`,
`KEYS` or prefix enumeration among the commands — so `teardown` cannot discover
what was written unless something recorded it.

* `track_keys_for_teardown: true` records each entity key in a per-view set, so
  `teardown` enumerates and deletes exactly the rows the view wrote. It costs
  one extra write per row to a single hot key, so it is off by default.
* With it off, `teardown` **logs a warning and leaves the rows**. It does not
  report success for work it did not do. Set `key_ttl_seconds` if you want rows
  to age out on their own.

## Wire-format notes worth knowing before you measure anything

The command surface is asymmetric about byte payloads, and it matters to
anyone reading a latency or bandwidth number off this path:

* **Requests are base64.** `HashMultiSet.entries` carries the bytes codec, so
  values go out base64 — a flat 1.33x.
* **Responses are decimal arrays.** `CommandResponse::Values` carries no such
  codec, so a byte comes back as the characters `104,`. That is roughly **3.8x
  the payload** for protobuf-serialized feature values. A read's wire cost is
  therefore about 4x its payload, and that is the **transport**, not the
  engine. The client decodes both shapes, so a node that gains the codec later
  needs no change here.

## Tests

```bash
cd integrations/feast
python feast_temporalstore_command/tests/test_online_store.py
```

Sixteen tests, no running store needed: the transport is replaced with a
recording stub and the assertions are about the commands this integration
emits. Four of them run with Feast absent; the rest skip loudly rather than
pass quietly. The runner asserts the collected test **count**, because a file
that stops being collected otherwise reports a green run of zero tests.

## Benchmark

`../benchmarks/bench_feature_serving.py` compares this store against SQLite,
Redis and PostgreSQL through Feast itself. It is store-agnostic — it selects an
arm by writing a `feature_store.yaml`, so it measures any Feast online store,
including the RESP path. See `docs/feast_online_store_benchmark.md` for the
method and the measured table.
