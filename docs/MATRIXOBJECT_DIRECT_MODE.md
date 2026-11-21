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
- It uses plain publication unless compact append is explicitly enabled.
- `compare_and_swap` is unsupported, as on the gateway.
- It is one in-process gateway per node; it is not tuned for many concurrent writers.

## Failure semantics

An append is never sent twice on an ambiguous result.

- **Definitive refusal** (a 412 that names a lost race, a 4xx): returned as the error; the
  caller decides.
- **Ambiguous** (timeout, connection reset, 503, an in-process door that failed to answer):
  the store reads back `[offset, offset+len)` and compares. Landed: the receipt is returned as
  if the first attempt had been acknowledged. Absent: the append is retried as a new append.
  Unreadable: `ObjectStoreError::OutcomeUnknown` is returned and nothing is repeated.
- **Leader changes** are the metadata client's: it rotates to another endpoint when one
  refuses, and a write is never sent again after it was submitted.

Residual, the same in both modes: a request still in flight inside a slow server can commit
after a read-back said "absent". Closing it needs the append fence (a no-op generation bump
published before a retry); the contract is the gateway-side "append fence" design. Until then, keep a
sequence number in the WAL payload so a duplicate is detectable.

## Bounded append publication

For a cluster whose metadata voters and journal readers support compact append
outcome versions 11 and 13, set `TS_MATRIXOBJECT_COMPACT_APPEND=1` on direct clients.
This also enables reservation for the next append. Set
`TS_MATRIXOBJECT_RESERVE_NEXT=0` to keep compact publication without that reservation.
Both settings accept `0`, `1`, `false`, or `true`. Reserving without compact
publication is refused before connecting.

Compact publication keeps the durable metadata decision proportional to the newly
written extents. A busy object's next allocation can share that decision. Enabling
it activates the cluster publication fence: all writers must use compatible staged
publication before this is enabled. Leave it off during a mixed-writer rollout.

TemporalStore still appends its binary protobuf WAL frame and its offset index
separately. These controls reduce metadata traffic; they do not remove either
required append or change uncertain-outcome recovery. Segment limits still bound
the manifest held by the client. Use the same segment position when comparing
single-append latency across corpus sizes, and report segment rotations separately.

Measure one durable WAL publication as the retained history grows:

```sh
TARGET=127.0.0.1:22111,127.0.0.1:22112,127.0.0.1:22113 \
COMPACT=true MILESTONES=256,1024,8192 VALUE=4096 \
cargo run --release -p temporalstore-rust --features matrixobject \
  --example matrixobject_single_append_growth
```

Run plain and compact modes against separate disposable clusters: a cluster's
publication fence persists. The report uses engine-produced binary outcomes,
appends each record individually, verifies indexed reads through a fresh reader,
and reports rotation costs separately from matched segment positions. Each sampled
record must issue exactly two object-store appends. The fixture has one writer; engine execution and ownership-lease checks are
outside the measured interval. A ratio printed by a busy-host run does not establish flat
latency; repeat on an idle host with a fixed memory limit and replica policy.
