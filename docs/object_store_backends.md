# Object Store Backends

The stream layer now has a URI-based backend boundary instead of hardcoding one remote object
store. This lets the same page, index, and oplog code choose storage by URI.

Redis is intentionally not listed here. This layer stores appendable blobs and stream metadata for
page/index/oplog persistence; Redis belongs at the serving protocol/cache layer, not the durable
object-store layer.

| URI scheme | Backend | Current status |
| --- | --- | --- |
| `file://...` | Local filesystem | Functional; used by local and smoke tests. |
| `blob://...` | MatrixObjectStore-compatible adapter | Functional when the compatibility library is linked. |
| `local://...` | MatrixObjectStore-compatible adapter | Functional when the compatibility library is linked. |
| `s3://bucket/prefix/...` | S3 adapter | API route exists; returns `Unimplemented` until an S3 SDK adapter is linked. |
| `ceph://bucket/prefix/...` | Ceph RGW through S3-compatible API | API route exists; returns `Unimplemented` until the S3 adapter is linked. |
| `ceph+s3://bucket/prefix/...` | Ceph RGW through S3-compatible API | Same as `ceph://`, but the compatibility mode is explicit. |
| `rados://pool/object...` | Native Ceph/librados | Reserved for a future native Ceph adapter. |

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
     -> blob://       MatrixObjectStore-compatible Store
     -> local://      MatrixObjectStore-compatible Store
     -> s3://         S3 Store adapter
     -> ceph://       S3 Store adapter against Ceph RGW
     -> ceph+s3://    S3 Store adapter against Ceph RGW
     -> rados://      Native Ceph Store adapter
```

The S3 and Ceph routes are intentionally wired as explicit unsupported backends today. That is
better than silently treating them as an invalid scheme: callers can already configure the right URI
shape, and the next implementation step is just replacing the stub with a concrete SDK-backed
`Store`.

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
