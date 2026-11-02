// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! S3-compatible Raft snapshot storage for the TemporalStore Rust rewrite.

#[path = "snapshot_metrics.rs"]
pub mod metrics;
pub mod object_store;
pub mod snapshot_store;
#[path = "snapshot_manifest_types.rs"]
pub mod types;

pub use metrics::SnapshotMetrics;
pub use object_store::{FileObjectStore, ObjectStore, ObjectStoreError};
pub use snapshot_store::{S3SnapshotStore, SnapshotStore, SnapshotStoreError};
pub use types::{
    ChecksumEntry, CompressionFormat, LocalSnapshot, BlockSlabManifest, SnapshotManifest,
    SnapshotRef, SnapshotRetention, SnapshotStatus,
};

/// Generated protobuf types for the snapshot manifest.
///
/// `pub(crate)`: the binary shape is an encoding detail of this crate's own objects, not part of
/// what it offers callers. The manifest's public type stays the Rust struct.
pub(crate) mod manifest_proto {
    include!(concat!(env!("OUT_DIR"), "/temporalstore.snapshot.v1.rs"));
}
