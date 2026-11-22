# MatrixObject: direct mode or a gateway

TemporalStore's shared store can reach a MatrixObject cluster two ways. Both give the same
TemporalStore behaviour and the same durable bytes: the same sequence of writes leaves the
same objects at the same generations.

| | gateway | direct |
|---|---|---|
| Setting | `TS_SHARED_STORE_URI=matrixobject://host:port` | `TS_MATRIXOBJECT_META=host:port[,host:port...]` |
| Mode switch | `TS_MATRIXOBJECT_MODE=gateway` | `TS_MATRIXOBJECT_MODE=direct` |
| Extra process | a gateway per cluster (or per few nodes) | none |
| Request path | node -> S3 over a socket -> gateway -> metadata group and chunk servers | node -> metadata group and chunk servers |
| Build | any | needs the `matrixobject` cargo feature |

Direct is the default. With no `TS_MATRIXOBJECT_MODE`, setting `TS_MATRIXOBJECT_META` selects
direct, even when `TS_SHARED_STORE_URI` is set too; a gateway is used by default only when it is
the only one configured (a node cannot reach a cluster directly without knowing its metadata
group). Set `TS_MATRIXOBJECT_MODE=gateway` to keep a gateway when both are set. A mode whose own
setting is missing is refused at start-up. A binary built without the `matrixobject` feature
refuses direct mode by name instead of falling back to a gateway.

## When to use which

- **Direct** when the node can reach the metadata group and the chunk servers and you want one
  hop fewer and no gateway to run, secure or keep alive. The gateway's request handling runs
  inside the node, so nothing about append, read-back, listing or ranged reads is different.
- **Gateway** when nodes cannot reach the chunk servers (network zones), when many nodes
  should share one set of cluster connections, or when the node binary is built without the
  `matrixobject` feature.

## What a direct node needs

- `TS_MATRIXOBJECT_META`: one to seven metadata endpoints, comma separated.
  `TS_MATRIXOBJECT_META_GROUP` names the group; several endpoints default to group 1.
- Optional: `TS_MATRIXOBJECT_BUCKET` (default `temporalstore`),
  `TS_MATRIXOBJECT_TIMEOUT_MS` (default 10000), and `MATRIXOBJECT_CLIENT_TLS_*` for mutual TLS.
- A formed metadata group and reachable chunk servers.
- Start-up is the probe: the node connects to the group and refuses to start, naming the
  endpoints, if none answers. It does not report ready with no metadata quorum.

## What direct mode does not do

- It does not register with the cluster node roster, answer health probes, or listen for
  other clients. There is no S3 listener.
- `compare_and_swap` is unsupported, as on the gateway.
- It is one in-process gateway per node; it is not tuned for many concurrent writers.

## Failure semantics

An append is never sent twice on an ambiguous result.

- **Definitive refusal** (a 412 that names a lost race, a 4xx): returned as the error; the
  caller decides.
- **Ambiguous** (timeout, connection reset, 503, a 409 `AppendAmbiguous`, an in-process door that
  failed to answer): every attempt carries its own `x-amz-attempt-id`, and the store asks the
  gateway about THAT attempt (`?append-resolve&attempt=`). Landed: the receipt is returned as if
  the first attempt had been acknowledged. Absent (the attempt is fenced out, so a late copy is
  refused): the append is retried as a new attempt with a new id. Ambiguous, or a gateway that
  cannot answer by attempt: `ObjectStoreError::OutcomeUnknown` is returned and nothing is
  repeated. The object is never read back to decide, because equal bytes at the offset do not
  prove they are this writer's.
- **Appends to one object from one process are serialised** by a lock per object key held
  around the append and its settlement, so tasks of one process do not race for the same end.
  The offset condition still arbitrates between processes.
- **Leader changes** are the metadata client's: it rotates to another endpoint when one
  refuses, and a write is never sent again after it was submitted.

## Checking both modes on a real cluster

`direct_wal_index_e2e` writes a shard's WAL through the shared store's own appender, rotates
segments, replays from the cluster alone on a fresh connection, trims, and appends concurrently,
through a gateway and through the in-process door, then compares what each left behind.

```text
GATEWAY=matrixobject://127.0.0.1:24410 META=127.0.0.1:24404,127.0.0.1:24405,127.0.0.1:24406 \
  TS_MATRIXOBJECT_META_GROUP=1 SEGMENTS=6 WRITERS=4 \
  cargo run --release -p temporalstore-rust --features matrixobject --example direct_wal_index_e2e
```

`MODES=gateway` or `MODES=direct` runs one mode, `WRITERS` sets the concurrent writers, and
`IDENTICAL=1` makes every writer append the same bytes, which is the case a byte comparison
cannot settle. The concurrent leg prints an `ANALYSIS` line: appends, acknowledged, errors,
records and bytes in the object, overlapping receipts, receipts that do not match the object.
It passes only with no errors, adjacent receipts that do not overlap, and the object holding
exactly what was acknowledged.

Measured on a three-node cluster whose gateway has attempt identity (2 and 4 writers, both
modes, distinct and identical payloads): all eight runs had no errors and every acknowledged
record in the object once, with no overlapping receipts. Against a gateway without attempt
identity, identical payloads were all acknowledged while the object held fewer records than
were acknowledged (50 acknowledged and 37 in the object at 2 writers), which is why an append
is settled by its attempt.

The run also prints one `FINDING` line: the shard lease cannot be acquired over the networked
store, because the store does not offer a conditional replace. Appends and replay do not need
it.

## Checking the object surface and killing servers mid-append

`direct_surface_cluster` has two legs.

- `LEG=surface` runs put, get, ranged get (inside, ending at, and past the end), list and
  list-after, delete and a read of the deleted object, and an append to an existing object, and
  prints one `OBS` line per call. Run it once per mode with the same `PREFIX` and `diff` the
  outputs. On a three-node cluster all 19 observations were identical through a gateway and
  directly.
- `LEG=append` appends records one at a time to one object and prints `PROGRESS`. While it runs,
  `scripts/direct_kill_leg.sh` SIGKILLs a server on a port and restarts it
  (`PORT=24401 RESTART='bash restart_chunk1.sh' ROUNDS=4 PAUSE=3 scripts/direct_kill_leg.sh`). At the
  end the example reads the object back and requires it to equal the acknowledged records, in order,
  each exactly once, and prints a `RESOLUTIONS landed=.. absent=.. ambiguous=..` line from the
  store's append-resolution counters (`append_resolutions()`).

The run says `INCONCLUSIVE` unless the kills produced both a `landed` and an `absent` answer, because
a kill that never interrupted an append, or that only ever produced one kind of answer, has not
exercised the settlement.

Measured on a three-node cluster, 1,500 appends per run, four kills per run:

| killed | mode | appends that returned an error | requests | resolutions | every record stored once | verdict |
|---|---|---|---|---|---|---|
| one chunk server | direct | 15 | 1,894 | landed 0, absent 126 | yes | INCONCLUSIVE (no `landed`) |
| one chunk server | gateway | 14 | 1,908 | landed 0, absent 131 | yes | INCONCLUSIVE (no `landed`) |
| each of the three metadata nodes | direct | 0 | 1,501 | none | yes | INCONCLUSIVE (no append was interrupted) |

So killing a chunk server exercised the `absent` settlement 126 and 131 times with no record lost or
duplicated; an append whose retries ran out returned an error to the caller, which read the object
and retried only when the record was not there. The `landed` answer was not produced in any run, and
killing a metadata node did not interrupt an append at all, so neither case is covered by this
measurement.

## Append publication

TemporalStore still appends its binary protobuf WAL frame and its offset index
separately. Neither required append is removed, and uncertain-outcome recovery is unchanged. Segment limits still bound
the manifest held by the client. Use the same segment position when comparing
single-append latency across corpus sizes, and report segment rotations separately.

Measure one durable WAL publication as the retained history grows:

```sh
TARGET=127.0.0.1:22111,127.0.0.1:22112,127.0.0.1:22113 \
MILESTONES=256,1024,8192 VALUE=4096 \
cargo run --release -p temporalstore-rust --features matrixobject \
  --example matrixobject_single_append_growth
```

Use a disposable cluster. The report uses engine-produced binary outcomes,
appends each record individually, verifies indexed reads through a fresh reader,
and reports rotation costs separately from matched segment positions. It also reports
all non-rotation appends in each growth phase, including their maximum latency, so
checkpoint tails outside the final sample window remain visible. Each sampled
record must issue exactly two object-store appends. The fixture has one writer; engine execution and ownership-lease checks are
outside the measured interval. A ratio printed by a busy-host run does not establish flat
latency; repeat on an idle host with a fixed memory limit and replica policy.

## Cluster only

MatrixObject is reached through a cluster, by a gateway or directly, and in no other way. The
node-local, in-process MatrixObject store has been retired, together with what only it used:

- `TS_MATRIXOBJECT_STORE_DIR` is **refused at start-up** by name. It used to point a node at a
  private on-disk snapshot, which is not shared storage whatever the bucket was called.
- `TS_MATRIXOBJECT_SYNC_FLUSH`, `TS_MATRIXOBJECT_FLUSH_INTERVAL_MS`, `TS_MATRIXOBJECT_FLUSH_BATCH`
  and `TS_MATRIXOBJECT_CHECKPOINT_ON_START` tuned that path and are no longer read. The cluster
  path publishes each write before it acknowledges it and has
  `TS_MATRIXOBJECT_NETWORKED_CHECKPOINT_ON_START`.
- `TS_STORAGE_BACKEND=matrixobject` without `TS_SHARED_STORE_URI` or `TS_MATRIXOBJECT_META` refuses
  to start, and `auto` no longer selects MatrixObject unless a configured
  `TS_MATRIXOBJECT_ENDPOINT` answers.
- The `matrixobject` cargo feature stays: direct mode needs it for the cluster client.
