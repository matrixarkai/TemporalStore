# MatrixObject Design Extraction And Readiness

## Source

- Source PDF: `C:/Users/Deeproute/Downloads/MATRIXOBJECTSTORE.pdf`
- PDF metadata: 26 pages, image-based, produced by jsPDF 2.3.1, no embedded text layer.
- Extraction method: rendered pages with Poppler and OCR with `tesseract -l chi_sim+eng`.
- Product naming: this repository uses `MatrixObject` as the public product/API name for the Rust-native object-store path. Historical names such as `MatrixObjectStore` and external source names in the PDF are treated as design ancestry or compatibility aliases, not the preferred public name for new code, docs, or APIs.

## Extracted Design Signals

The OCR output is noisy because the PDF is image-only, but the first-page extraction and visible table of contents consistently identify these design themes:

- A distributed blob/object storage system for large binary objects.
- Four production criteria: durability, availability, scalability, and operability.
- Binary object payloads are treated as bytes; the store does not interpret application payload semantics.
- Metadata management is explicit and separate from payload bytes.
- User semantics include append/write-oriented blob storage and random-read access.
- CRC/checksum validation and background integrity scanning are required for reliability.
- High availability depends on metadata failover, lease/ownership rules, and server registration/discovery.
- QoS, throttling, and multi-tenant isolation are first-class operational requirements.
- Object lifecycle includes chunk/block placement, extent/blob metadata, versioning, failover, and garbage collection.

## Current Implementation Coverage

`temporalstore-snapshot::MatrixObjectStore` remains the Rust type name for the local production backend, while `MatrixObject` is the public provider/API name exposed in reports and contracts. The backend currently provides:

- Async `put`, `get`, `list`, and `delete` object operations.
- Atomic local-file publish by writing a temporary object in the target directory, `sync_all`, and renaming into place.
- `ObjectMetadata` with stable key, URI, size, and SHA-256 checksum.
- `head` and `put_atomic` APIs for metadata-first callers.
- Path traversal rejection for object keys.
- Temporary-file filtering during prefix listing.
- Raw-message spill contract using canonical `matrixobject://...` object refs, with `matrixobjectstore://...` and `blob://...` accepted as legacy aliases.
- Raw-message object refs that include payload size and SHA-256 checksum.
- A provider-neutral object-store adapter contract shared with S3: `put`, `put_atomic`, `put_unique`, `put_if_absent`, `put_path_unique`, `get`, `get_range`, `get_to_path`, `head`, `list`, `list_page`, `delete`, `delete_objects`, `delete_prefix`, `copy_object`, `uri`, `capabilities`, and `topology`.
- TemporalStore-owned metadata rows for S3/MatrixObject payloads, unless MatrixKV is explicitly selected as the metadata backend.

## Production Readiness Criteria

MatrixObject is ready for the current local/shared-store target when all of these are true:

- Payload writes are atomic and never expose partial object bytes.
- Object refs carry checksum metadata and can be validated by readers.
- Metadata is queryable from TemporalStore or MatrixKV without scanning object payload storage.
- Large resource/raw-message payloads spill to MatrixObject or S3 according to size policy.
- Default raw-message writes are cold-store/no-promotion.
- Object listing is prefix-scoped and excludes temporary publish artifacts.
- Missing, invalid, and deleted objects are distinguishable from IO failures.
- Shared-store sync/async replay can read payload refs after restart.

## Remaining Gaps

- Live remote object-store backends still need real endpoint validation.
- Background integrity scanning is not yet a continuous MatrixObject runtime.
- Multi-tenant QoS and throttling are policy-level today, not a standalone object-store scheduler.
- Object version history and conditional write conflict APIs are still limited.
- Cross-language C++/Rust executable cases should be expanded for object metadata, checksum validation, conditional write, delete/list, and restart replay.

## Naming Rule

Use `MatrixObject` in new docs, reports, and public APIs. Keep `MatrixObjectStore` only where it is already a concrete Rust type or backward-compatible URI/backend alias. New TemporalStore shared-store integrations should consume the generic object-store adapter contract first, then choose `matrixobject://`, `s3://`, or another provider URI at configuration time. Selection should be by URI scheme plus reported capabilities, with unlinked remote providers failing closed instead of partially pretending to write.
