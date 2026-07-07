# BCache Design PDF vs TemporalStore C++ Gap Analysis

Date: 2026-07-07

Source PDF: `C:\Users\Deeproute\Downloads\BCACHE.pdf`

Code reviewed: `/root/src/github-services/TemporalStore`, C++ server/client/proxy/metaserver/storage tree.

Note: the PDF is image-based, so this analysis is based on rendered page inspection. It should be treated as an engineering comparison against the visible architecture, storage, checkpoint, consistency, and client/proxy diagrams in the PDF.

## Executive Summary

TemporalStore C++ is directionally aligned with the BCache design. It already has a real distributed skeleton: metaserver metadata, namespace/table/partition state, partition placement, proxy/client routing, object/index/oplog/page storage, log-based streams, block cache, local raft/snapshot support, and several business modules.

It is not yet equivalent to the full BCache design. The biggest gaps are:

1. production-grade multi-AZ and multi-region replication;
2. complete incremental checkpoint and object-stream lifecycle;
3. production-grade data-node raft snapshot/install/restore;
4. partition split/merge and hot-object relocation;
5. full Redis/model breadth;
6. follower-read/session-consistency gates;
7. compression/erasure coding;
8. mature placement, rebalance, QoS, and hot-key invalidation.

## What The BCache PDF Describes

The PDF describes a distributed cache/storage system with these major concepts:

- Logical model: namespace, table, object, key, model.
- Deployment model: region, IDC/AZ, rack, host/server, group.
- Data model: hash number, hash range, partition, piece, stream.
- Architecture: SDK, cache proxy, cache core/server, memory store, shared storage.
- Storage hierarchy: memory, PMEM, NVMe SSD, HDD/shared storage.
- Partitioning: cluster -> namespace -> table -> partition -> piece -> object.
- Replication: primary partition handles writes; backup partitions can serve reads after applying logs/checkpoints.
- Multi-AZ: primary in one IDC/AZ and backups in other IDC/AZs.
- Multi-region: independent regional clusters with cross-region DTS-style log/data replication.
- Metaserver: node/client/table/partition management, placement, balancing, failover, QoS, metadata recovery.
- Storage engine: object indexes, object pool, oplog buffer, object buffer, object streams, index streams, oplog streams.
- Checkpoint: incremental checkpoint, index sliding windows, hot/cold log separation, object dump/rewrite, oplog trim.
- Recovery: restore index, replay oplog from checkpoint, recover quickly.
- Consistency: read-after-write, eventual consistency option, session consistency via client token/sequence.
- Models: String, List, Hash, Set, ZSet, Json, TimeSeries, and Redis-compatible variants.
- Client/proxy: protocol wrapper, service discovery, routing, load balancing, hot-key invalidation, aggregation, transactions.

## What TemporalStore C++ Already Has

### Distributed Metadata And Topology

TemporalStore C++ has a metaserver v2 tree with namespaces, tables, partition sets, partitions, servers, proxies, location metadata, failure detection, publishing, and raft-backed metadata state.

Relevant code:

- `src/metaserver_v2/meta/metabase.cc`
- `src/metaserver_v2/meta/namespace.cc`
- `src/metaserver_v2/meta/table.cc`
- `src/metaserver_v2/meta/partition.cc`
- `src/metaserver_v2/meta/server.h`
- `src/metaserver_v2/meta/location.h`
- `src/metaserver_v2/meta_publisher.cc`

This maps well to the PDF's namespace/table/partition/server/proxy control plane.

### Metaserver Raft And Snapshot

Metaserver raft support and local snapshot support are present. The code can dump/load metabase snapshots and trigger snapshots through the raft server path.

Relevant code:

- `src/metaserver_v2/fsm.cc`
- `src/metaserver_v2/raft_server.h`
- `src/metaserver_v2/service/raft_control_service.cc`
- `src/metaserver_v2/trivial_routine.cc`
- `src/metaserver_v2/meta/metabase.cc`

This is one of the stronger areas versus the PDF, at least for local metadata recovery.

### Storage Skeleton

TemporalStore C++ has a storage architecture that resembles the PDF:

- object manager;
- oplog;
- page store;
- index;
- stream abstraction;
- log-based stream implementation;
- block cache.

Relevant code:

- `src/partition/storage/object_manager.h`
- `src/partition/storage/op_logger.h`
- `src/partition/storage/page_store.h`
- `src/partition/index/index.h`
- `src/stream/stream.h`
- `src/stream/log_based_stream_base.cc`
- `src/blockcache/blockcache.h`

The pieces line up with the PDF's object pool, object index, object stream, index stream, oplog stream, and cache layers.

### Object Load/Evict And TTL Basics

Object loading, deletion, TTL, dirty tracking, and index/page state exist.

Relevant code:

- `ObjectManager::GetObject`
- `ObjectManager::LoadObject`
- `ObjectManager::DeleteObject`
- `ObjectManager::SetObjectTtl`
- `ObjectManager::DoExpireObject`
- `Index::EvictSlot`
- `Index::MarkSlotDataDirty`
- `Index::MarkSlotPageDirty`

This covers part of the PDF's object load/evict/delete/TTL behavior.

### Proxy And Client Routing

TemporalStore has client/proxy code, metaserver topology sync, routing, and proxy service methods.

Relevant code:

- `src/proxy/service.cc`
- `src/proxy/proxy.h`
- `src/client/client_impl.cc`
- `src/client/meta_syncer.h`
- `src/client/router_v2.h`

This partially maps to the PDF's SDK/proxy/service-discovery/routing layer.

### Existing Models And Extensions

TemporalStore has several modules:

- string;
- hash;
- set extension;
- feature;
- IPS;
- risk;
- temporal aggregate;
- context.

Relevant code:

- `src/extension/string`
- `src/extension/hash`
- `src/extension/set`
- `src/extension/feature`
- `src/extension/ips`
- `src/extension/risk`
- `src/extension/temporal_aggregate`
- `src/extension/context`

This is useful, but not full PDF model parity.

## Detailed Missing Areas

### 1. Multi-Region Active-Active And DTS Replication

PDF expectation:

- independent clusters per region;
- DTS nodes replicate logs/data across regions;
- regional read/write policy;
- eventual cross-region consistency;
- active-active or active-read/write strategy.

Current C++ status:

- Location metadata has region/VDC-like concepts.
- I did not find a complete cross-region DTS/log replication pipeline.
- I did not find conflict resolution, cross-region replay cursors, cross-region lag metrics, or cutover logic.

Gap:

TemporalStore C++ needs a real cross-region replication subsystem if it wants parity with the PDF. Metadata naming alone is not enough.

Needed work:

- cross-region log export/import;
- per-table replication policy;
- replay cursor/checkpoint;
- conflict policy;
- cross-region lag metrics;
- disaster recovery runbook and tests.

### 2. Multi-AZ Backup Serving And Promotion Gates

PDF expectation:

- primary partition handles writes;
- backup partitions apply logs/checkpoints from shared storage;
- backups can serve reads;
- promotion/failover is safe across AZs.

Current C++ status:

- There are raft/shared-store/secondary visibility pieces and examples.
- There are lag and replication smoke benchmarks.
- Production gates are still incomplete.

Gap:

Follower/secondary read safety is not fully proven. Promotion must be gated by applied index, snapshot freshness, storage generation, and log replay position.

Needed work:

- bounded-stale read SLA;
- read-after-write token support;
- secondary applied-index metrics;
- failover tests during sustained writes;
- promotion gate based on committed/applied index;
- stale replica fencing.

### 3. Data-Node Raft Snapshot Production Readiness

PDF expectation:

- replicas can recover from checkpoint and oplog;
- far-behind replicas can install snapshot;
- recovery is bounded and safe under write pressure.

Current C++ status:

- Metaserver snapshots are relatively mature locally.
- Data-node raft snapshot support exists, but the production path is still weaker than metaserver.
- Current data-node snapshot behavior is mostly local-file oriented.

Gap:

Data raft needs a hardened install-snapshot and restore path for real shared storage/object storage, not only local filesystem scenarios.

Needed work:

- snapshot manifest with object/index/oplog generation IDs;
- atomic install with validation;
- object-store/S3 shared-file support;
- restore while writes continue;
- far-behind replica snapshot catch-up;
- fail-closed behavior for stale local state.

### 4. Incremental Checkpoint And Index Sliding Windows

PDF expectation:

- avoid full data rewrite;
- checkpoint changed objects only;
- maintain index sliding windows;
- trim obsolete oplog;
- delete invalid object/index files;
- separate hot and cold logs/streams.

Current C++ status:

- Oplog, page store, index, dirty tracking, stream, and block cache primitives exist.
- Full checkpoint lifecycle is not clearly complete.
- Index/window/object-dump orchestration is not at PDF level.

Gap:

TemporalStore needs a real checkpoint manager that coordinates oplog sequence, object dump, index window, object stream rewrite, and safe trim.

Needed work:

- checkpoint state machine;
- incremental object-dump queue;
- index window creation/finalization;
- checkpoint manifest;
- oplog trim guard;
- object stream garbage collection;
- recovery objective tests.

### 5. Stream Restore Gap

PDF expectation:

- RecordStore-style stream abstraction can create/open/delete/append/read/scan/restore.

Current C++ status:

- `Stream::RestoreInfo` exists in `src/stream/stream.h`.
- `src/stream/log_based_stream.h` has a `RestoreInfo` implementation that asserts `not supported`.
- Reader/base restore paths exist, but the unsupported implementation is still a red flag.

Gap:

Stream metadata restore needs to be consistent across writer/reader paths. This matters for snapshot install, recovery, and moving partitions.

Needed work:

- implement or remove unsupported `RestoreInfo` path;
- add crash/reopen tests;
- add snapshot restore tests using restored stream metadata;
- verify stream tail scan and CRC behavior after restore.

### 6. Partition Split/Merge

PDF expectation:

- freeze old partition;
- create new partitions;
- rewrite/index data into new hash ranges;
- restore IO;
- support merge of adjacent ranges.

Current C++ status:

- Freeze/update partition metadata exists.
- Balance/move concepts exist.
- I did not find full split/merge data rewrite and validation.

Gap:

TemporalStore lacks the complete operational split/merge pipeline described by the PDF.

Needed work:

- split planner;
- merge planner;
- hash-range scan/rewrite;
- shadow validation;
- routing cutover;
- rollback if validation fails;
- active traffic tests.

### 7. Piece-Level Compute And Hot Object Relocation

PDF expectation:

- partition can be divided into pieces;
- pieces map to worker threads;
- hot objects can move to dedicated workers;
- location manager tracks object placement.

Current C++ status:

- Partition/server/threading infrastructure exists.
- I did not see a complete hot-object location manager with object-level worker migration.

Gap:

The CPU scaling model is less complete than the PDF. It likely scales by partitions/modules, but not by full piece/hot-object relocation semantics.

Needed work:

- piece abstraction if not already complete;
- hot-object detector;
- object-to-worker location map;
- safe migration protocol;
- client/proxy invalidation when placement changes;
- hot-key stress tests.

### 8. Placement, Rebalance, And Scale Up/Down

PDF expectation:

- placement considers server load, host/rack/IDC constraints, replica isolation;
- rebalance moves partitions safely;
- scale up/down is deterministic.

Current C++ status:

- Balance routine and balance task exist.
- The implementation appears simple/brute-force in places.

Gap:

Production placement needs a stronger policy engine and deterministic validation harness.

Needed work:

- rack/host/AZ anti-affinity;
- load-aware scoring;
- bounded partitions moved per round;
- safe-gap policy;
- rebalance freeze timeout;
- add/remove node harness with active writes/reads;
- Prometheus metrics for movement/freeze/failure.

### 9. Full Model And Redis Parity

PDF expectation:

- String, List, Hash, Set, ZSet, Json, TimeSeries;
- Redis-compatible variants of those models.

Current C++ status:

- String/hash/custom feature/risk/IPS/temporal modules exist.
- Full Redis protocol and complete List/ZSet/Json/TimeSeries parity are not obvious in the current C++ tree.

Gap:

TemporalStore C++ is strong for custom serving/feature workloads, but not yet a complete Redis-compatible BCache-style model surface.

Needed work:

- List model and commands;
- ZSet model and commands;
- Json model and commands;
- TimeSeries model and commands;
- Redis protocol gateway;
- compatibility tests against Redis command semantics.

### 10. Session Consistency And Conditional Updates

PDF expectation:

- read-after-write;
- eventual consistency option;
- session consistency with client token/sequence;
- conditional update token/CAS-like behavior.

Current C++ status:

- Stronger paths exist via primary/write routing and raft/shared-store modes.
- I did not find a complete public read-token/session-consistency API.

Gap:

Consistency modes need to be explicit and testable at client/proxy/API level.

Needed work:

- read token returned on writes;
- read token accepted on reads;
- bounded stale follower reads;
- CAS/update-token API;
- tests for stale reads, failover, and retry.

### 11. Compression And Erasure Coding

PDF expectation:

- compression for stored data;
- avoid over-compressing hot objects;
- erasure coding for storage efficiency.

Current C++ status:

- Some module-level compression concepts exist.
- Topology response has compressed payload support.
- Oplog comments indicate page data is not compressed.
- I did not find storage-layer erasure coding.

Gap:

General object/oplog/index stream compression and EC are not implemented at PDF level.

Needed work:

- per-stream compression policy;
- hot/cold compression policy;
- compression metrics;
- EC storage layout;
- rebuild/read path for EC shards;
- fault-injection tests.

### 12. Client/Proxy Hot-Key Cache Invalidation

PDF expectation:

- hot keys can be cached in client/proxy;
- writes trigger invalidation.

Current C++ status:

- Client/proxy topology and routing exist.
- I did not find a complete hot-key invalidation protocol.

Gap:

Hot-key client/proxy caching is not yet production-complete.

Needed work:

- hot-key cache metadata;
- write invalidation broadcast;
- versioned object tokens;
- stale cache rejection;
- metrics and tests.

### 13. QoS And Multi-Tenant Isolation

PDF expectation:

- per namespace/table throttling;
- quota;
- resource isolation;
- load-aware scheduling.

Current C++ status:

- Token bucket/quota/backpressure pieces exist.
- Proxy has ingestion controls.

Gap:

QoS is partial, not a full namespace/table/server-wide isolation system.

Needed work:

- namespace/table QPS quotas;
- byte/write/read quotas;
- tenant admission control;
- noisy-neighbor tests;
- per-tenant Prometheus metrics;
- server-side enforcement, not only proxy-side enforcement.

### 14. Proxy As Full Protocol Gateway

PDF expectation:

- SDK and proxy expose compatible protocols such as Redis;
- clients can switch proxies on failure;
- proxy handles routing/load balancing.

Current C++ status:

- Proxy service exists and calls TemporalStore client implementation.
- It exposes selected commands and ingestion paths.

Gap:

Proxy is not yet a full Redis-compatible gateway.

Needed work:

- RESP/Redis protocol support;
- command compatibility table;
- proxy failover tests;
- retry metrics;
- backpressure behavior under proxy failover.

### 15. Persistent Memory And Generic Persistent Containers

PDF expectation:

- persistent array/list/stack/queue/tree/hash map/set;
- memory persistence manager APIs;
- PMEM/page-pool style persistence.

Current C++ status:

- TemporalStore has object/page/index abstractions.
- I did not find a full generic persistent container layer matching the PDF.

Gap:

The generic persistent-memory container library is not implemented at the PDF level.

Needed work:

- decide whether PMEM/generic containers are still required;
- if yes, implement persistent container abstractions;
- if no, document the replacement design using object/page/index streams and cache.

## Production Readiness Priority

### P0

- Harden data-node raft snapshot install/restore.
- Add restore-under-write-pressure tests.
- Add follower-read freshness gates.
- Add stale local data fencing.
- Implement checkpoint manifest and oplog trim safety.
- Fix/complete stream `RestoreInfo` path.

### P1

- Implement incremental checkpoint manager.
- Implement rebalance/add/remove node harness with active traffic.
- Implement partition split/merge planning and validation.
- Add secondary lag metrics and promotion gates.
- Add namespace/table QoS enforcement.

### P2

- Full Redis/model parity: List, Set, ZSet, Json, TimeSeries.
- Full proxy protocol gateway.
- Hot-key invalidation.
- Compression and hot/cold stream policy.

### P3

- Multi-region DTS replication.
- Erasure coding.
- Generic persistent memory containers, if still needed.

## Bottom Line

TemporalStore C++ has the right base architecture for a BCache-like system, especially in metadata, partitioning, object/index/oplog/page storage, and local raft/snapshot work.

The code is missing the full production machinery from the PDF: robust checkpoint lifecycle, snapshot install/restore, split/merge, hot relocation, follower-read/session consistency, full model/protocol breadth, multi-region replication, and storage efficiency features such as compression and erasure coding.

The next engineering push should focus on checkpoint/failover first. Without that, the higher-level scale, follower-read, rebalance, and multi-region pieces cannot be made safely production-ready.
