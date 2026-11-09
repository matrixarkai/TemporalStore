// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum FeatureFilterOp {
    Equal = 0,
    NotEqual = 1,
    GreaterThan = 2,
    LessThan = 3,
    GreaterOrEqual = 4,
    LessOrEqual = 5,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum FeatureWritePolicy {
    Upsert = 0,
    Block = 1,
    First = 2,
    Update = 3,
    /// Merge the cells each point carries into the snapshot stored at its timestamp. The value
    /// is a cell-set request (header followed by cells). The in-process door cannot carry it:
    /// its C string argument is lossy for binary values, so that door refuses this policy.
    MergeReduce = 4,
}

/// What a merge-reduce append landed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct CellMergeReceipt {
    /// Snapshots written: one per distinct timestamp in the request.
    pub snapshots_written: u64,
    /// Cells that did not exist at their timestamp before.
    pub cells_created: u64,
    /// Cells folded into a member already stored, or repeated within the request.
    pub cells_merged: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct FeatureFilter {
    pub field: String,
    pub op: FeatureFilterOp,
    pub value: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct FeaturePoint {
    pub timestamp_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct SequenceFeatureRow {
    // The engine's row names this `timestamp_ms` and requires it. The Rust name stays as it was
    // so callers do not change; only the wire name is corrected.
    #[cfg_attr(feature = "proxy", serde(rename = "timestamp_ms"))]
    pub timestamp: u64,
    pub gid: u64,
    pub action_type: u32,
    pub duration: u32,
    pub author_id: u64,
}
