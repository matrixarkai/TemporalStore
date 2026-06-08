# Fault Tolerance

## Current Rust Coverage

The Rust implementation models the TemporalStore fault-tolerance path locally:

- data-node Raft write replication requires majority
- leader loss promotes a caught-up follower
- writes continue after promotion when quorum remains
- writes are rejected when quorum is lost
- lagging followers are rejected for read-index and leader transfer
- followers catch up from leader logs after outage
- new replicas can be added and bootstrapped from current state
- replicas can be removed while preserving majority
- local Raft snapshots can bootstrap lagging data and meta replicas
- Raft install-snapshot requests can carry external S3/MinIO snapshot manifest references with
  checksum and byte size metadata
- stale snapshots cannot overwrite newer local state
- leader election does not depend on S3 snapshot availability
- metaserver Raft metadata remains readable/writable after leader failover with quorum
- metaserver Raft rejects reads and writes after quorum loss
- proxy/client E2E tests refresh routes after backend failure
- local restart tests prove page-address indexes reload data from local page files
- shared-store tests restore index/page data and replay later oplog records

## Critical Behavior

Normal write path:

```text
client/proxy -> data-node leader -> Raft majority commit -> local engine apply -> followers apply -> response
```

Data-node leader failure:

```text
leader down -> Raft group promotes best live caught-up follower -> writes continue if majority remains
```

Metaserver leader failure:

```text
meta leader down -> meta Raft promotes live follower -> shard metadata mutations continue if majority remains
```

Quorum loss:

```text
majority unavailable -> writes rejected -> read-index rejected -> relaxed local reads are the only non-linearizable option
```

Replica bootstrap:

```text
new/lagging replica -> install local/S3 snapshot -> catch up later Raft logs -> serve follower reads only after read-index safety check
```

## Still Not Production Fault Tolerance

The local model is useful for API and correctness shape, but it is not yet a production distributed system. Remaining production work:

- replace the local consensus model with OpenRaft or raft-rs
- replace the local auto-persisting Raft WAL model with OpenRaft or raft-rs storage integrated into
  real multi-process consensus
- add real network transport for data-node and metaserver Raft
- implement crash-recovery tests that kill and restart OS processes
- wire external S3 snapshot references into real download, verification, and install-snapshot
  streaming across processes
- add multi-process chaos tests for packet loss, process kill, disk-full, slow follower, and rolling upgrade
- add production failure detection and placement decisions in metaserver
