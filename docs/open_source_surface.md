# TemporalStore Open-Source Surface

TemporalStore keeps a smaller public build surface behind
`BCACHE2_OPEN_SOURCE_SURFACE`.

The open-source build keeps:

- Basic Redis-compatible commands: auth/ping/info/command metadata, minimal
  string commands, minimal key lifetime commands, and minimal hash commands.
- MatrixArk context-management serving data models: nodes, events, child
  indexes, secondary indexes, entities, summaries, embeddings, dirty markers,
  and compression events.
- Feature data model.
- Single Risk data model for frequency-cap and risk-control.

Only the model families listed above are public in the first open-source release. Context audit/replay/debug data models are not part of the first-release open-source surface. Other internal model families are intentionally omitted from docs, manifests, and compatibility corpora until they are ready for a future public release.

The open-source build excludes non-public/internal model families and extension
modules that are not part of the first-release public contract. It also does not expose Redis server-configuration or broad
keyspace/collection-clone commands such as `CONFIG`, `DBSIZE`, broad `KEYS` /
`SCAN`, `SADD`, `LPUSH`, or `ZADD`. `HSCAN` is excluded from the first-release public surface. The set protobuf may still compile as a
compatibility helper for legacy Redis handler code, but the set module is not
registered in the public surface. Full internal builds remain unchanged when
`BCACHE2_OPEN_SOURCE_SURFACE` is off.

Rust Redis command execution also supports a runtime guard:

- `TEMPORALSTORE_OPEN_SOURCE_SURFACE=1`
- `TS_OPEN_SOURCE_SURFACE=1`

When enabled, unsupported Redis/module commands fail closed before execution.

The canonical Redis API contract is `compat/redis_open_source_surface_manifest.json`. MatrixObject is a shared object-store backend below TemporalStore storage/backfill; enabling MatrixObject must not expand the public Redis API into set/list/zset collection-clone commands or storage-provider-specific commands.

Run the policy validator after changing this surface:

```bash
python3 tools/validate_open_source_surface.py
```
