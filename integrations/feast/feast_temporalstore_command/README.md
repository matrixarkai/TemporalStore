# TemporalStore as a Feast online store — over the native command surface

A [Feast](https://github.com/feast-dev/feast) **online store** that talks to a
TemporalStore data node over its own command surface: binary
shared-field `POST /batch_hash_multi_set_msgpack` writes and
compact `POST /batch_hash_multi_get_compact_msgpack` reads, with compatible
shared-field and command-batch routing for augmented writes and older nodes.

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
  max_batch_commands: 512
  max_write_batch_commands: 4096
  max_batch_request_bytes: 4194304
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
   becomes N sequential round trips. This package groups entity reads into
   bounded `/batch_hash_multi_get_msgpack` requests.
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

`online_read` over N entity keys uses ordered binary
`/batch_hash_multi_get_compact_msgpack` requests and responses bounded by key
count and encoded request bytes. The projection is encoded once per batch
instead of repeating every feature field beside every entity key. Successful
responses carry one top-level status, positional value rows, and sparse indexed
errors instead of one status and response tag per entity. Nodes without the
compact route are detected once per client and use
`/batch_hash_multi_get_msgpack`, `/batch_execute_msgpack`, or `/batch_execute`.
The answers are positional; a short answer is refused
rather than zipped, because a missing slot would shift every value onto the
wrong feature and read downstream as a stale feature rather than as an error.
Repeated requested feature names are collapsed before the projection is sent,
so they do not add wire bytes or protobuf decoding work. Version-tagged feature
views use isolated field namespaces and are supported by both direct and Feast
orchestrated reads.

An entity that was never written reads back as `(None, None)`, not as a dict of
empty values.

## Writes

`online_write_batch` sends the common latest-value batch through
the packed timestamp-set routes: field names are encoded once, followed by
ordered `(entity key, timestamp, values)` rows. TTL writes use a distinct route
and fall back to timestamp reads plus TTL-bearing hash commands when that route
is unavailable. The serving node must support the combined command; older
nodes reject it rather than silently retaining keys. The server compares timestamps,
writes accepted rows, and arms their optional key expiry under one shard guard;
stale rows neither write data nor refresh their expiry. This removes the
timestamp-read round trip and a separate expiry command from the common TTL
materialization path. Mixed feature layouts and writes augmented with history
or teardown tracking use bounded binary `/batch_write_msgpack` command batches.
When TTL is configured, each hash write in those batches carries its own
expiry; it does not need a second command for the same entity.
A batched timestamp read runs first only when stale-write suppression is
requested and the atomic route is unavailable. That timestamp projection also uses
the compact shared-field read route, so `_ts` crosses the wire once per batch.
Reads retain the 512-command response bound while writes
default to 4,096 commands and remain capped at 4 MiB of encoded request data.
Nodes without either binary write route are detected once per client and use
`/batch_write` or `/batch_execute`:

* **`skip_dedup: false` (default)** — read each row's stored `_ts` field, then
  write only the rows whose event time is newer. The number of round trips is
  bounded by batch size, not by the number of rows. This stops an out-of-order materialization
  moving a feature backwards, and it also deduplicates *within* the call, so
  two rows sharing an entity key keep the newer one regardless of which was
  queued last.
* **`skip_dedup: true`** — skips the timestamp read. Right for an initial load or an
  append-only pipeline whose rows arrive in order.

### `write_feature_history`

Off by default. When on, every value is *also* appended to a feature series
keyed by `(view, entity, feature)` at its event timestamp, so the history
behind the served value stays queryable through `FeatureQuery`. Scalar Int32
and Int64 values also get an ASCII-integer projection for native windowed
`FeatureAggQuery`; the raw serialized history remains unchanged. The adapter's
`query_feature_aggregate` accepts `sum`, `avg`, `min`, `max`, `first`, `last`,
`count`, and `events`. Numeric operations require an integer feature; counts
read the raw series. Existing history written before the numeric projection
needs backfilling before numeric queries can include it.

For write-heavy history workloads, set `compact_numeric_history: true` alongside
`write_feature_history: true`. This stores each numeric value only in the raw
serialized series and sends numeric windows through `FeatureProtoAggQuery`.
The serving node must support that command; older nodes reject it rather than
returning a value computed from the wrong encoding. Compact numeric windows
scan their retained points, whereas the default numeric projection can use
the aggregate rollup. Keep the default for read-heavy window queries.

`query_feature_aggregates` sends many windows through bounded ordered batches.
The repeat-gated 1,024-entity benchmark measured a median 4,207 aggregate
queries/s at batch 64, with every full-window and tail-window result verified.

Repeated events for the same entity-feature within one Feast write batch are
consolidated into one multi-point history command. The latest-value hash is
also written once per entity, using the newest accepted event in that batch.
This preserves every history timestamp without paying three commands per event.

Serving reads do not change: `online_read` always answers from the hash. That
is deliberate, and it is worth knowing why. `FeatureQuery` returns a window in
**ascending** timestamp order and truncates with `take(count)`, so asking it
for "the latest point" costs O(points in the window), not O(1). It is a history
read, not a latest-value read. Turning this flag on adds one history append
per feature series in a write batch and leaves the read path alone.

## Teardown

TemporalStore's command surface has **no key-space scan** — there is no `SCAN`,
`KEYS` or prefix enumeration among the commands — so `teardown` cannot discover
what was written unless something recorded it.

* `track_keys_for_teardown: true` records each entity key in a per-view set, so
  `teardown` enumerates and deletes this view's hash fields and both history
  series layouts, including data written before a numeric-history mode change. Tracking
  is distributed over 1,024 deterministic sets to keep each container small.
  Teardown pages those sets with exclusive cursors and bounded responses; it
  requires a serving node with `SetScan` support.
  It still costs one extra write per row, so it is off by default.
* With it off, `teardown` **logs a warning and leaves the rows**. It does not
  report success for work it did not do. `key_ttl_seconds` expires the latest
  entity hash and, for new history writes, each history series. History TTL
  requires a serving node that supports `FeatureAppendWithTtl`; older nodes
  reject the command rather than retaining history without the requested TTL.

## What of Feast's `OnlineStore` contract is covered

**Tested against `feast==0.67.0`.** Read from that version's
`OnlineStore` class rather than from its documentation, because the two differ:
only four methods are abstract, and several of the rest raise instead of
no-opping, which decides whether inheriting them is safe or a latent failure.

Partial is written as partial below. Nothing here is rounded up.

| method | required? | status |
| --- | --- | --- |
| `online_write_batch` | **abstract** | **implemented** — bounded `/batch_write` calls with a compatibility fallback; parameters identical to the base class |
| `online_read` | **abstract** | **implemented** — bounded binary batch calls for N entities with a compatibility fallback |
| `update` | **abstract** | **implemented** — nothing to create (the surface is schemaless); views being removed are swept through the teardown path |
| `teardown` | **abstract** | **partial** — exact only with `track_keys_for_teardown`, which spreads keys over 1,024 deterministic sets and is off by default because it adds one write per row. Async commit is supported. With tracking off, teardown logs that it cannot enumerate instead of claiming success |
| `online_read_async` | optional, **base raises** | **implemented** — without it `get_online_features_async` fails outright |
| `online_write_batch_async` | optional, **base raises** | **implemented** |
| `close` | optional, base is `pass` | **implemented** — the base no-op leaks a keep-alive connection per thread |
| `plan` | optional, base returns `[]` | **inherited, and correct** — a schemaless surface needs no infra objects |
| `initialize` | optional, base is `pass` | inherited; nothing to initialise |
| `retrieve_online_documents`, `retrieve_online_documents_v2` | optional, **base raises** | **not implemented — vector search is not supported by this provider** |
| `read_precomputed_vectors`, `write_precomputed_vector` | optional, base has generic feature-row implementations | **implemented** — writes the compatible synthetic feature field without an unused timestamp and reads repeated entities through one deduplicated compact projection |
| `get_online_features`, `get_online_features_async` | provided by Feast | inherited and exercised, including version-tagged views; compatible feature views are coalesced into one compact projection per entity set |

### Against Feast's own process for a new online store

Feast's `docs/how-to-guides/customizing-feast/adding-support-for-a-new-online-store.md`
lists six steps. Where they apply to a package outside Feast's tree:

| step | status |
| --- | --- |
| 1–2. `OnlineStore` + `OnlineStoreConfig` classes, `OnlineStore` suffix | done; Feast resolves `type:` by import path and imports `<ClassName>Config` from the same module, so no entry-point registration is needed |
| 3. reference it from `feature_store.yaml` | done, documented above |
| 4. test it against Feast's suite | **done for the online-store suite, and it passes.** `feast_repo_configuration.py` and `feast_online_store_creator.py` supply `AVAILABLE_ONLINE_STORES`, `FULL_REPO_CONFIGS` and an `OnlineStoreCreator` that starts a data node. Feast 0.67.0's own `tests/integration/online_store/test_universal_online.py`, run against this store, reports **5 passed, 14 skipped, 1 error** — see the breakdown below. Plus `tests/test_feast_api_conformance.py` here: five tests driving the provider through Feast's public API against a live node |
| 5. dependencies | done; `pyproject.toml` keeps Feast itself optional |
| 6. documentation | this file, plus the benchmark write-up |

Feast's bar for a plugin to stop being "contrib" is its own CI running the full
suite against the store, plus two owning contributors. **Neither is met**, so
treat this as alpha.

### Running the conformance tests

```bash
cd integrations/feast
# starts its own node from target/release/server, tears it down after
PYTHONPATH="$PWD" python feast_temporalstore_command/tests/test_feast_api_conformance.py
# or point at a node you already run -- it must be an EMPTY shard
TEMPORALSTORE_TEST_ADDR=127.0.0.1:17602 PYTHONPATH="$PWD" python \
  feast_temporalstore_command/tests/test_feast_api_conformance.py
```

Exit codes are three distinct outcomes, not two: `0` passed, `1` failed, **`2`
skipped entirely because no node was reachable, which is not a pass.**

### Feast's universal online-store suite: the measured result

`tests/integration/online_store/test_universal_online.py` from Feast 0.67.0,
run against this store with the hooks above: **5 passed, 14 skipped, 1 error.**
The parameterisation confirms it was ours —
`LOCAL:File:TemporalStoreOnlineStoreCreator:python_fs:False`.

**Passed (5):** `test_online_retrieval_success`,
`test_online_retrieval_with_event_timestamps`, `test_online_list_retrieval`,
`test_online_retrieval_with_shared_batch_source`, `test_write_to_online_store`.

**Skipped (14), and the reason is the same for every one of them:** `got empty
parameter set ['environment']`. They are not skipped because this store failed —
Feast gates them on fixtures that this configuration does not provide. They are
mostly vector-search (`test_retrieve_online_documents`, `..._v2`,
`..._milvus_documents`, `test_openai_search*`, `test_retrieve_online_image_search_hybrid`)
which this provider does not support anyway. **Three of them are worth wanting
and are not yet covered:** `test_entity_ttl_online_store`,
`test_online_store_cleanup` and `test_async_online_retrieval_with_event_timestamps`.

**Error (1):** `test_async_online_retrieval_with_event_timestamps_dynamo` — a
DynamoDB-specific test, unrelated to this store.

### Running it yourself, and the four things that block it

None of these are this store's doing; they are what Feast 0.67.0's test tree
needs, and each was found by being **named** rather than guessed:

1. **`pytest<8`.** Feast pins `pytest>=6.0.0,<8`. On pytest 9 the run dies with
   `INTERNALERROR ... Unknown config option: env`, then `timeout`, because
   pytest 9 fails strictly on options the plugins Feast pins register.
2. **`FEAST_IS_LOCAL_TEST=True`.** Without it `repo_configuration.py` imports
   BigQuery and Redshift fixtures at module scope. This is Feast's own switch.
3. **`-p no:benchmark`.** `pytest-benchmark<4` needs the legacy `py.io`, which
   modern `py` no longer provides. The online-store tests do not use it.
4. **`google-cloud-bigquery`, for an import that is never used.**
   `test_universal_online.py` imports one name, `prep_file_source`, from
   `tests/utils/data_source_test_creator.py` — a tempfile-and-parquet helper
   that never touches BigQuery. That module nevertheless does
   `from google.cloud import bigquery` at module scope for two other functions
   the online-store suite does not import, so the whole suite is unrunnable
   without a cloud SDK it has no need for. Install it, or stub the unused
   import for the run. **The result above was obtained with that import
   stubbed**, which is sound only because `prep_file_source` does not use it.

```bash
pip install 'pytest>=7,<8' pytest-env pytest-timeout pytest-mock \
            pytest-asyncio assertpy minio testcontainers deltalake \
            boto3 python-keycloak grpcio 'ibis-framework[duckdb]'
# from a Feast 0.67.0 checkout, in sdk/python:
FEAST_IS_LOCAL_TEST=True IS_TEST=True \
PYTHONPATH=".:/path/to/integrations/feast" \
TEMPORALSTORE_SERVER_BIN=/path/to/target/release/server \
FULL_REPO_CONFIGS_MODULE=feast_temporalstore_command.feast_repo_configuration \
PYTEST_PLUGINS=feast_temporalstore_command.feast_online_store_creator \
  python -m pytest -p no:benchmark -q --integration \
  tests/integration/online_store/test_universal_online.py
```

## Wire-format notes worth knowing before you measure anything

The command surface is asymmetric about byte payloads, and it matters to
anyone reading a latency or bandwidth number off this path:

* **Binary batch requests keep byte strings binary.** For 4,096 commands with
  12,296-byte values, the encoded request stream measured 50,605,338 bytes,
  versus 67,460,469 bytes through JSON: 24.99% fewer wire bytes. Median client
  encoding time was 55.41 ms versus 273.01 ms for the exact same command set.
* **Binary batch responses keep byte strings binary.** A measured response
  carrying ten 12 KiB values is 123,551 bytes, versus 164,811 bytes through
  JSON base64: 25.03% fewer wire bytes. The compatibility routes still encode
  requests as JSON base64, and the client accepts JSON base64 and the older
  decimal-array response shape.

## Tests

```bash
# both provider packages, with or without Feast installed
python integrations/feast/run_tests.py

# or this package's suite on its own
cd integrations/feast && PYTHONPATH="$PWD" python feast_temporalstore_command/tests/test_online_store.py
```

**The current offline run executes 52 checks:** 45 command-provider tests plus
7 wire tests. The transport is replaced with a recording stub in that tier;
the five-case Feast API conformance tier runs separately against a live node.

The runner asserts the **executed** count, not the collected one, and that
distinction is the whole guard: `unittest` collects a class decorated with
`skipUnless` whether or not the condition holds, so a collected count cannot
tell a run with Feast from a run without it — and cannot tell either from a run
where the Feast tier silently stopped executing.

`run_tests.py` additionally runs the sibling `feast_temporalstore` package's
wire tests, which are bare `test_*` functions with no runner of their own, and
it needs no third-party test runner so the open-source readiness job can
execute both tiers without installing one.

## Benchmark

`../benchmarks/bench_feature_serving.py` compares this store against SQLite,
Valkey and PostgreSQL through Feast itself. It is store-agnostic — it selects an
arm by an `online_store` block, so it measures any Feast online store,
including the RESP path. See
[`../../../docs/feast_online_store_benchmark.md`](../../../docs/feast_online_store_benchmark.md)
for the method, what the harness refuses to report, and the measured tables.
The accepted 4,096-entity long-sequence run served 12,296-byte values at
12,289 async and 6,204 durable warm rows/s at batch 10; eight-reader throughput
was 4,585 and 9,344 rows/s. The control drift was 2.1%, every sampled value was
exact, and teardown returned both stores from 4,096 records to zero. The
repeat-gated entity-scale sweep with a fixed 512-row hot set served 21,070 rows/s
at 5,000 resident rows and about 10,400 rows/s at both 20,000 and 50,000 rows;
repeat drift stayed below 2.9% and every value matched.
