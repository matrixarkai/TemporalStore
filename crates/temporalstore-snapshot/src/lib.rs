//! S3-compatible Raft snapshot storage for the TemporalStore Rust rewrite.

pub mod metrics;
pub mod object_store;
pub mod snapshot_store;
pub mod types;

pub use metrics::SnapshotMetrics;
pub use object_store::{
    FileObjectStore, MatrixObjectBlockRef, MatrixObjectManifest, MatrixObjectStore,
    MatrixObjectStoreBackendMode, MatrixObjectStoreBlockService, MatrixObjectStoreChunkService,
    MatrixObjectStoreConfig, MatrixObjectStoreRootService, ObjectMetadata, ObjectStore,
    ObjectStoreError, SharedObjectStore, SharedObjectStoreBackend, SharedObjectStoreConfig,
};
pub use snapshot_store::{S3SnapshotStore, SnapshotStore, SnapshotStoreError};
pub use types::{
    ChecksumEntry, CompressionFormat, LocalSnapshot, PageSegmentManifest, SnapshotManifest,
    SnapshotRef, SnapshotRetention, SnapshotStatus,
};
