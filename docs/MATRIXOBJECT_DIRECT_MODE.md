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

Setting both settings without a mode is refused at start-up, as is a mode that contradicts a
setting.

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
- It runs the gateway's default publication path (no compact append, no next-append
  reservation).
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
