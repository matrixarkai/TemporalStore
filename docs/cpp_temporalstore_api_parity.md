# C++ TemporalStore API Parity

Reference sources checked:

- `/home/vj/src/temporalstore/src/protocol/*module.proto`
- `/home/vj/src/temporalstore/src/extension/*/interface.proto`
- local TemporalStore deep-dive docs under `Documents/Codex/2026-05-10/bytekv-in-local-vs-etcd`

## Implemented In Rust

Common:

- `DEL_OBJECT` -> `CommonDelete`
- `EXPIRE` -> `CommonExpire`
- `TTL` -> `CommonTtl`
- Redis-compatible `EXISTS`
- C++ `common2::Expire` first resolves the object and returns not-found for missing keys; Rust now
  matches that engine behavior. The RESP adapter preserves Redis shape by translating missing
  `EXPIRE` to integer `0`.

String:

- `SET` -> `StringSet`
- `SETEX` -> `StringSetEx`
- `GET` -> `StringGet`
- Redis-compatible `MGET`, `MSET`, `GETDEL`, `DEL`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`

Hash:

- `SET` -> `HashSet`
- `GET` -> `HashGet`
- `DEL` -> `HashDelete`
- `MGET` -> `HashMultiGet`
- `MSET` -> `HashMultiSet`
- `GETALL` -> `HashGetAll`
- `LEN` -> `HashLen`
- `INCRBY` -> `HashIncrBy`, including C++-style rejection of non-integer values and i64 overflow
- Redis-compatible `HSET`, `HGET`, `HDEL`, `HMGET`, `HMSET`, `HGETALL`, `HLEN`, `HINCRBY`

Set:

- `SADD` -> `SetAdd`
- `SMEMBERS` -> `SetMembers`
- Rust additionally has `SetRemove` / Redis `SREM`

Feature:

- `ADD` -> `FeatureAppend`
- write-policy add -> `FeatureAppendWithPolicy` with `upsert`, `insert_if_absent`, and
  `replace_existing`
- C++ add hard limit is enforced before mutation: current feature size plus incoming
  points greater than `100000` returns `invalid_argument`; the normal retained window
  still defaults to `feature_max_size = 5000` after successful writes.
- `QUERY` -> `FeatureQuery`
- C++ protobuf `FeaturePoint` value filtering -> `FeatureQueryFiltered`
- `AGGQUERY` -> `FeatureAggQuery`
- `REPLACE` -> `FeatureReplace`
- `DEL` -> `FeatureDelete`
- Typed client coverage: `feature_append`, `feature_append_with_policy`, `feature_query`,
  `feature_query_filtered`, `feature_replace`, `feature_delete`, `feature_agg_query`
- RESP coverage: `FAPPEND`, `FAPPENDPOLICY`, `FQUERY`, `FQUERYFILTER`, `FREPLACE`, `FDEL`, `FAGG`
- Rust can encode/decode the C++ `feature::FeaturePoint` protobuf value shape
  (`gid`, `action_type`, `duration`, `author_id`) without generated C++ proto code.
  `FeatureQueryFiltered` decodes those stored point bytes, applies typed filters matching the
  C++ fields, and returns the original point bytes in timestamp order.
- C++ `QueryRequest.filters` string syntax is supported through
  `FeatureFilter::parse_cpp_filter`, client `feature_query_cpp_filters`, and RESP
  `FQUERYFILTERSTR`; examples match C++ filters like `gid = 1`, `duration < 4`,
  and `author_id != 9`. Repeated filters on the same field follow the C++
  `std::map` behavior: the later filter overwrites the earlier one for that
  field.

Sequence:

- add rows -> `SequenceAdd`
- query rows -> `SequenceQuery`
- batch query rows -> `SequenceBatchQuery`
- Typed client coverage: `sequence_add`, `sequence_query`, `sequence_batch_query`

IPS:

- `ADD` -> `IpsAdd`
- dimension/idempotent add -> `IpsAddWithOptions`
- query last -> `IpsQueryLast`
- range query -> `IpsQueryRange`
- dimension-filtered range query -> `IpsQueryRangeWithOptions`
- batch query last -> `IpsBatchQueryLast`
- remove timestamp -> `IpsRemove`
- delete key -> `IpsDelete`
- count range -> `IpsCount`
- load point snapshot -> `IpsLoad`
- snapshot range -> `IpsSnapshot`
- range stats -> `IpsStat`
- named dimension filter -> `IpsFilter`
- Typed client coverage: `ips_add`, `ips_add_with_options`, `ips_query_last`,
  `ips_query_range`, `ips_query_range_with_options`, `ips_batch_query_last`,
  `ips_remove`, `ips_delete`, `ips_count`, `ips_load`, `ips_snapshot`, `ips_stat`,
  `ips_filter`
- RESP coverage: `IPSADD`, `IPSADDOPT`, `IPSQUERYLAST`, `IPSQUERYRANGE`,
  `IPSQUERYRANGEOPT`, `IPSBATCHQUERYLAST`, `IPSREMOVE`, `IPSDEL`, `IPSCOUNT`,
  `IPSLOAD`, `IPSSNAPSHOT`, `IPSSNAPSHOTREPORT`, `IPSSTAT`, `IPSFILTER`
- Production snapshot metadata -> `IpsSnapshotReport`, including range metadata, returned versus
  total counts, action/table aggregations, and packed page evidence for the timestamped page blocks
  backing the snapshot.

Risk:

- increment -> `RiskIncrement`
- precision/TTL increment -> `RiskIncrementWithOptions`
- count/sum window -> `RiskCount`
- aggregate query -> `RiskQuery` with `sum`, `min`, `max`, `first`, `last`, and `events`
- detail list -> `RiskDetail`
- C++-named family commands -> `RiskSet`, `RiskFamilyQuery`, `RiskSetAndGet`, and
  `RiskManager` for the `h`, `cpc`, and `fol` risk families
- C++ FOL first/last string semantics -> `RiskFolSet` and `RiskFolQuery`, preserving the selected
  value by event timestamp rather than treating FOL as a numeric sum-only family
- Typed client coverage: `risk_increment`, `risk_increment_with_options`, `risk_count`,
  `risk_query`, `risk_detail`, `risk_family_set`, `risk_family_query`,
  `risk_family_set_and_get`, `risk_fol_set`, `risk_fol_query`, and `risk_manager`
- RESP coverage: `RISKINCR`, `RISKINCROPT`, `RISKCOUNT`, `RISKQUERY`, `RISKDETAIL`
  plus C++-style `RISKHSET`, `HQUERY`, `HSETANDGET`, `CPCSET`, `CPCQUERY`,
  `CPCSETANDGET`, `FOLSET`, `FOLQUERY`, `FOLSETANDGET`, and `RISKMANAGER`.
  `FOLSET key value occur_time_ms ttl_ms FIRST|LAST` and `FOLQUERY key` now model the C++ string
  first/last behavior; the older numeric FOL test shape remains for compatibility with the local
  simplified family shim.
  `RISKHSET` is used for H-family set-only writes in RESP so normal Redis `HSET`
  remains hash-compatible.

Runtime/control surface:

- load/unload shard
- config get/set
- stats/info
- metaserver namespace/table create, update, open/topology, close, and delete lifecycle
- membership update with C++-style stale global/unit version rejection and local active-membership reporting
- page/index stream read and scan
- in-process Raft behavior model
- S3-compatible snapshot store abstraction

Redis operational/admin compatibility:

- `AUTH`
- `BGSAVE`
- `CONFIG GET`
- `CONFIG SET`
- `CONFIG REWRITE`
- `INFO`
- `SLAVEOF`
- `PARTITION LOAD`
- `PARTITION UNLOAD`
- `PARTITION INFO`
- CRC64 slot/hash helpers through `PSLOTHASHKEY`, `PCLUSTERKEYSLOT`, and `PCLUSTERHASH`

## Partially Implemented

IPS:

- Rust covers add, dimension/idempotent add, query-last, range query, dimension-filtered range
  query, batch query-last, remove timestamp, delete key, count range, local load, range snapshot,
  snapshot metadata reports, stats, and named filter, with typed client and RESP coverage.
- C++ IPS still has deployment-specific snap internals, but the Rust surface now covers production
  snapshot metadata and server-side action/table aggregation for the local open-source model.

Risk:

- Rust covers increment, precision/TTL increment, count, sum/min/max/first/last/event aggregation,
  and detail lists, with typed client and RESP coverage.
- C++ risk has `HSET`, `HQUERY`, `CPCSET`, `CPCQUERY`, `FOLSET`, `FOLQUERY`,
  `HSETANDGET`, `FOLSETANDGET`, `CPCSETANDGET`, and `MANAGER`. Rust now covers
  those command shapes for local integer-window behavior, C++-style FOL first/last string selection,
  and a manager summary.
- Missing C++ semantics include deeper CPC/list-specific internals and production
  manager/debug operations beyond the local summary.

Feature:

- Rust supports point append/query/replace/delete, write-policy append, and simple
  count/sum/min/max aggregation.
- Rust now exposes and tests a `feature_sequence_cpp_proto_v1` golden corpus through
  `cpp_feature_sequence_golden_corpus_report()`. The corpus covers the C++ protobuf value
  shape, duplicate-field filter replacement, filtered feature query, empty aggregate behavior,
  sequence filtering, and packed timestamped KV page layout.
- Rust also exposes `cpp_api_golden_corpus_v1` through `cpp_api_golden_corpus_report()`.
  This broader Rust-local corpus combines the feature/sequence golden cases with Redis-compatible
  string/hash/set core commands, IPS filter/stat/snapshot behavior, Risk family/FOL/manager
  behavior, and admin storage-readiness checks after mixed API writes.
- C++ feature API includes richer `FeaturePoint` structure with nested point arrays, time ranges,
  and richer aggregate query behavior. Rust currently stores one value per timestamp.

String:

- C++ `SetRequest` has `nx_flag` and `xx_flag`; Rust now supports equivalent
  `StringSetConditional` plus Redis-compatible `SET NX/XX GET EX/PX`.

## Missing Production Runtime Features

The C++ deep dive describes a full serving engine. The Rust rewrite has closed several of the
older gaps, but it is still not a drop-in production replacement for C++ TemporalStore.

Covered or substantially covered in Rust now:

- C++ `crc64 >> 34` table routing formula in the Rust client router, plus an opt-in C++
  `PartitionId` bit-layout helper for table partition ids.
- Shard-affine data-node worker lanes with bounded foreground/background queues, per-shard FIFO
  execution, cross-shard parallelism, and foreground priority over background work.
- Append-only local oplog and index-log streams, replica replay cursors, replay gap checks, and GC
  retention boundaries for oplog/index-log/page segments.
- Readonly secondary behavior: readonly startup mode, writes rejected with `readonly_shard`, remote
  HTTP primary stream source, `/replica/replay`, background replay loop, metaserver-discovered
  primary route, replay backoff, status endpoint, and Prometheus loop metrics.
- Server registration plus periodic heartbeat/load reporting to metaserver, including per-partition
  `partition_info` stats.
- Local per-shard admission control: `maxmemory_bytes`, `read_qps`, and `write_qps`.
- Prometheus scrape output from data-node/server paths for shard records, cache, page-store, oplog,
  runtime queues/jobs/dirty objects, and replica replay loop status.

Partially covered, but still materially smaller than C++:

- Metaserver namespace/table topology, table partitions, topology versioning, server/proxy
  inventory, heartbeat, guarded table update plus delete/dropped-state lifecycle, local mutation-log
  replay, optional in-process Raft-backed mutation path, load-aware/location-diverse replica fill,
  rebalance model, and scheduler/task models exist. The
  full C++ namespace/table/partition-set placement hierarchy and placement-rule chain are still not
  complete.
- Hot object state is represented by per-type maps of key/field/timestamp to `PageAddress`, with
  logical object/page-ref/dirty-object/dirty-slot stats. Rust still does not clone the full C++
  `ObjectManager` memory layout with stable object ids, page ids, slot ownership, and lifecycle.
- Cache is a Rust multi-layer read-through cache over memory, local page files, and page-address
  blocks. It is not blockcache/mtcache-compatible and does not provide CacheLib-style SSD admission,
  eviction, warmup, pinning, and observability.
- Shared-store replication exists for file/object-store checkpoint, page, index, and oplog flows.
  There is still no production ByteStore stream backend parity.
- Raft has local/distributed model coverage, HTTP transport contracts, WAL persistence, snapshots,
  external snapshot refs, membership safety models, and local harnesses. It is still not production
  ByteRaft parity and still lacks real OpenRaft/raft-rs FSM/storage integration, actual mTLS
  transport implementation, networked metaserver-driven data-Raft membership scheduling, and
  external multi-process/host chaos validation.

Still intentionally missing from the open-source Rust target:

- brpc/thrift SDK wire compatibility. The Rust target uses HTTP/JSON admin/debug, RESP compatibility,
  and a Rust client API today; tonic/gRPC remains the intended open internal RPC path.
- Full C++ dashboards/runbooks/production alerting package. Prometheus metric output exists, but the
  complete dashboard and operations bundle is not done.

## Current Conclusion

The Rust repo now covers the main simple module APIs: common, string, hash, set, feature,
sequence, and the implemented IPS/Risk subset with typed client and RESP coverage. It is not yet
feature-complete versus the full C++ TemporalStore product, mainly because exact C++ proto
semantics, routing/topology, C++ slot-owned dump/load recovery, OpenRaft/raft-rs integration,
mTLS/tonic production surfaces, external chaos validation, and production replication are still
large subsystems rather than small API aliases.
