# Object Store Backends

The stream layer now has a URI-based backend boundary instead of hardcoding one remote object
store. This lets the same page, index, and oplog code choose storage by URI.

Redis is intentionally not listed here. This layer stores appendable blobs and stream metadata for
page/index/oplog persistence; Redis belongs at the serving protocol/cache layer, not the durable
object-store layer.

| URI scheme | Backend | Current status |
| --- | --- | --- |
| `file://...` | Local filesystem | Functional; used by local and smoke tests. |
| `matrixobject://...` | MatrixObject adapter | Functional when the compatibility library is linked. |
| `matrixobjectstore://...` | Legacy MatrixObject alias | Backward-compatible alias for older configs. |
| `blob://...` | Legacy MatrixObject-compatible adapter | Functional when the compatibility library is linked. |
| `local://...` | Legacy MatrixObject-compatible adapter | Functional when the compatibility library is linked. |
| `s3://bucket/prefix/...` | S3 adapter | Functional for unsigned `http://` endpoints such as fake-S3/MinIO smoke tests; authenticated HTTPS still needs an SDK adapter. |
| `ceph://bucket/prefix/...` | Ceph RGW through S3-compatible API | Same adapter shape as S3; unsigned `http://` endpoints work, authenticated deployments still need SDK/auth wiring. |
| `ceph+s3://bucket/prefix/...` | Ceph RGW through S3-compatible API | Same as `ceph://`, but the compatibility mode is explicit. |
| `rados://pool/object...` | Native Ceph/librados | Reserved for a future native Ceph adapter. |

## Rust MatrixObject Boundary

Rust exposes `MatrixObjectStore`, `MatrixObjectStoreConfig`, and
`MatrixObjectStoreBackendMode` from `temporalstore-snapshot`. The default Rust
mode is `LocalCompat`, which uses the canonical `matrixobject://` URI contract
and atomic object-store semantics on top of a local durable directory.
`External` mode records the MatrixObject endpoint in config so the same
snapshot/shared-store code can switch to a native MatrixObject client when
that crate is linked.

This keeps Rust and C++ on the same public object-store contract:

- `matrixobject://...` identifies MatrixObject-backed durable objects.
- `matrixobjectstore://...` remains a backward-compatible legacy alias.
- `MatrixObject` is the public product/API name.
- `MatrixObjectStore` remains the Rust/C++ implementation type name for compatibility.
- Retired legacy object-store naming is not part of public APIs, docs, build
  flags, or validation output.

## Generic ObjectStore Contract

TemporalStore callers should code against the `ObjectStore` trait and
`SharedObjectStoreConfig`, not against MatrixObject-specific implementation
types. The generic contract now exposes the operations needed by MatrixObject,
S3-compatible stores, Ceph RGW, local files, and future object backends:

- `put` / `put_atomic`: publish a complete object and return metadata.
- `put_unique` / `put_path_unique`: append-style uploads for snapshots, WAL,
  and oplog objects that do not need overwrite cleanup.
  MatrixObject writes large path uploads through chunk manifests; the unsigned
  S3-compatible path streams file bytes into one create-only `PUT` request.
- `put_if_absent`: conditional create for append-style objects. MatrixObject and
  file-backed stores reject existing keys with `AlreadyExists`; the unsigned
  HTTP S3-compatible path maps this to `If-None-Match: *`, and SDK-backed
  adapters should use the same create-only semantic or an equivalent condition.
- `get` / `get_to_path`: read a complete object into memory or directly to a
  destination path.
  MatrixObject uses direct chunk-manifest operations; the unsigned
  S3-compatible path streams downloads into a temporary sibling and atomically
  moves it into place.
- `get_range`: read only a byte range, matching the natural S3 ranged-GET
  model and MatrixObject chunk manifests.
- `head`: return key, URI, size, SHA-256 metadata, and optional opaque object
  validators such as `etag` and `version_id` without requiring callers to know
  whether the backend stores manifests, local files, or remote object metadata.
  MatrixObject returns `etag=sha256:<checksum>` plus a manifest-derived
  `version_id`; S3-compatible stores preserve ETag/version headers when the
  endpoint provides them.
  Remote S3-compatible writes, path transfers, and metadata reads preserve those
  validators whenever the backend returns them.
- `list` / `list_page` / `delete` / `delete_objects`: full or paginated prefix
  listing and single-key or batch object deletion. Production scans should
  prefer `list_page` so S3 and MatrixObject prefixes do not have to materialize
  every key before making progress.
  MatrixObject implements this at the manifest/root service boundary and returns
  continuation tokens in the public object-key namespace, not internal manifest
  file names.
  The unsigned S3-compatible fake path exercises S3-style `max-keys` plus
  `continuation-token` pagination so remote adapters share the same contract.
- `copy_object`: copy one object to another key. MatrixObject copies into
  destination-owned chunks so deleting the source cannot break the copy; S3
  adapters should map this to server-side copy when available.
- `delete_prefix`: delete all objects matching a prefix through paged listing
  and `delete_objects`, using backend-native bulk delete when available.
  The S3-compatible adapter maps `delete_objects` to one `POST ?delete`
  multi-object request for unsigned HTTP endpoints.
- `capabilities`: report support for atomic put, unique put, conditional create,
  path upload, path download, metadata head, prefix list, paginated list,
  delete, bulk delete, object copy, prefix deletion, byte-range read, checksum,
  opaque object validators such as ETags, object version IDs when available,
  and split services.
- `topology`: report a generic service list. MatrixObject maps this to
  root/block/chunk services; local file and shared file map to one object
  service; S3-style adapters should map to one remote object service unless a
  deployment has a richer split.
- `topology.namespace` and `topology.key_prefix`: expose remote object location
  pieces generically. For example, `s3://bucket/prefix` reports
  `namespace=bucket` and `key_prefix=prefix`; `rados://pool/path` reports
  `namespace=pool` and `key_prefix=path`. Local and MatrixObject-compatible
  stores leave these fields empty.

This keeps the TemporalStore side generic: storage code can select by URI,
inspect capabilities, and use byte-range reads without hardcoding MatrixObject
internals. MatrixObject remains the optimized implementation that can split
large objects into root manifests, block refs, and chunks behind the same
adapter API.

Remote backends that are not linked yet, such as `s3://`, `ceph+s3://`, and
`rados://`, still instantiate as generic remote adapters so callers can inspect
their backend identity, URI scheme, endpoint, expected capabilities, runtime
link status, and topology through the same API. Their data operations fail
closed with `UnsupportedBackend` until the concrete SDK/client implementation is
linked. This lets planning and report code compare MatrixObject and S3-style
adapters through one contract without accidentally performing remote writes.

## Why Ceph Should Use S3 First

Ceph RGW exposes an S3-compatible object API, so the first production implementation should be a
single S3 adapter that can talk to AWS S3, MinIO, and Ceph RGW. That keeps the code small and avoids
vendoring Ceph into the storage engine. Native `rados://` can be added later only if we need lower
latency or features that RGW cannot expose.

## Implementation Boundary

`StoreLayer` calls `DetectObjectStoreBackend(uri)` and dispatches to a `Store` implementation:

```text
stream/page/index/oplog
     -> StoreLayer
     -> file://       LocalFileStore
     -> matrixobject:// MatrixObject Store
     -> matrixobjectstore:// MatrixObject legacy alias
     -> blob://       MatrixObject legacy alias
     -> local://      MatrixObject legacy alias
     -> s3://         S3 Store adapter
     -> ceph://       S3 Store adapter against Ceph RGW
     -> ceph+s3://    S3 Store adapter against Ceph RGW
     -> rados://      Native Ceph Store adapter
```

C++ also exposes `ObjectStoreBackendCapabilityReport`, a provider-neutral
capability summary for the same backend families. It reports canonical backend
name, canonical URI scheme, runtime-linked status, fail-closed status,
condition metadata support, prefix listing, metadata stat/head, append/open
write support, byte-range read support, delete, copy/rename, split services,
S3 compatibility, and local/shared-file compatibility. The capability report is
intentionally separate from MatrixObject internals so stream/page/index/oplog
planning can compare MatrixObject, S3-compatible stores, local files, and
future object-store backends through one shape.

The Rust remote adapter now has a backend-neutral request planner for S3-compatible stores.
It maps `s3://bucket/prefix`, `ceph://bucket/prefix`, and `ceph+s3://bucket/prefix` plus an
endpoint into path-style object URLs for put/get/head/delete, prefix listing, and copy-source
headers. Unsigned `http://` endpoints use this path for basic object operations today, which covers
local fake-S3 and simple MinIO-style smoke tests without adding a large SDK dependency. Missing
endpoints, `https://` endpoints, authenticated S3, and native `rados://` still fail closed until the
concrete SDK/client implementation is linked.

## Next Implementation Step

Build one `S3Store` with the existing `Store` contract:

- `SetCondition` and `StatCondition`: store condition blobs as small side objects.
- `Open(kWrite)`: create a multipart writer with append buffering.
- `Open(kRead)`: use range reads.
- `List`: list by prefix.
- `Delete`, `Stat`, `Rename`, `Freeze`: map to object operations; `Rename` is copy plus delete for
  S3-compatible stores.

Ceph RGW can reuse the same adapter with endpoint, region, access key, and path-style addressing
configuration. Native `rados://` should remain a separate, optional adapter.
