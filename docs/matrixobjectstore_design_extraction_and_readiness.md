# MatrixObjectStore Design Extraction And Readiness

## Source

- Source PDF: `C:/Users/Deeproute/Downloads/BYTESTORE.pdf`
- PDF metadata: 26 pages, image-based, produced by jsPDF 2.3.1, no embedded text layer.
- Extraction method: rendered pages with Poppler and OCR with `tesseract -l chi_sim+eng`.
- Product naming: this repository uses `MatrixObjectStore` for the Rust-native object-store path. Historical or external source names in the PDF are treated as design ancestry, not the public name for new code, docs, or APIs.

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

`temporalstore-snapshot::MatrixObjectStore` is the named Rust local production backend for object-store semantics. It currently provides:

- Async `put`, `get`, `list`, and `delete` object operations.
- Atomic local-file publish by writing a temporary object in the target directory, `sync_all`, and renaming into place.
- `ObjectMetadata` with stable key, URI, size, and SHA-256 checksum.
- `head` and `put_atomic` APIs for metadata-first callers.
- Path traversal rejection for object keys.
- Temporary-file filtering during prefix listing.
- Raw-message spill contract using `matrixobjectstore://...` object refs.
- Raw-message object refs that include payload size and SHA-256 checksum.
- TemporalStore-owned metadata rows for S3/MatrixObjectStore payloads, unless MatrixKV is explicitly selected as the metadata backend.

## Production Readiness Criteria

MatrixObjectStore is ready for the current local/shared-store target when all of these are true:

- Payload writes are atomic and never expose partial object bytes.
- Object refs carry checksum metadata and can be validated by readers.
- Metadata is queryable from TemporalStore or MatrixKV without scanning object payload storage.
- Large resource/raw-message payloads spill to MatrixObjectStore or S3 according to size policy.
- Default raw-message writes are cold-store/no-promotion.
- Object listing is prefix-scoped and excludes temporary publish artifacts.
- Missing, invalid, and deleted objects are distinguishable from IO failures.
- Shared-store sync/async replay can read payload refs after restart.

## Remaining Gaps

- Live remote object-store backends still need real endpoint validation.
- Background integrity scanning is not yet a continuous MatrixObjectStore runtime.
- Multi-tenant QoS and throttling are policy-level today, not a standalone object-store scheduler.
- Object version history and conditional write conflict APIs are still limited.
- Cross-language C++/Rust executable cases should be expanded for object metadata, checksum validation, conditional write, delete/list, and restart replay.

## Naming Rule

Use `MatrixObjectStore` in new Rust code, docs, reports, and APIs. Avoid introducing any new public names with a temporary or experimental suffix.
