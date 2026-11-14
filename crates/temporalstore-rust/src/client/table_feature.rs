// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreTable feature-point + sequence methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn feature_append(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::FeatureAppend {
            key: key.into(),
            points,
        })
    }

    pub fn feature_append_with_policy(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
        policy: FeatureWritePolicy,
    ) -> Result<bool, ClientError> {
        match self
            .execute(Command::FeatureAppendWithPolicy {
                key: key.into(),
                points,
                policy,
                retry_token: None,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_append_with_policy",
                response,
            }),
        }
    }

    /// Appends cell-set requests under the merge-reduce policy and says what landed:
    /// `(snapshots written, cells created, cells merged)`. A request that disagrees with the
    /// header stored for the key is refused and nothing is written.
    pub fn feature_merge_cells(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
    ) -> Result<(u64, u64, u64), ClientError> {
        match self
            .execute(Command::FeatureAppendWithPolicy {
                key: key.into(),
                points,
                policy: FeatureWritePolicy::MergeReduce,
                retry_token: None,
            })?
            .response
        {
            CommandResponse::CellMergeReceipt {
                snapshots_written,
                cells_created,
                cells_merged,
                ..
            } => Ok((snapshots_written, cells_created, cells_merged)),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_merge_cells",
                response,
            }),
        }
    }

    pub fn feature_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::FeatureQuery {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_query",
                response,
            }),
        }
    }

    pub fn feature_query_filtered(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: Vec<FeatureFilter>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::FeatureQueryFiltered {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                filters,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_query_filtered",
                response,
            }),
        }
    }

    pub fn feature_query_filters(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: &[String],
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        let filters = parse_feature_filters(filters.iter().map(String::as_str))
            .map_err(ClientError::InvalidRequest)?;
        self.feature_query_filtered(key, start_ms, end_ms, count, filters)
    }

    pub fn feature_replace(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        points: Vec<FeaturePoint>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::FeatureReplace {
            key: key.into(),
            start_ms,
            end_ms,
            points,
        })
    }

    pub fn feature_delete(&self, key: impl Into<String>) -> Result<(), ClientError> {
        self.expect_empty(Command::FeatureDelete { key: key.into() })
    }

    pub fn feature_agg_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
        count: Option<usize>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::FeatureAggQuery {
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
                count,
            })?
            .response
        {
            CommandResponse::Aggregate { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_agg_query",
                response,
            }),
        }
    }

    pub fn sequence_add(
        &self,
        key: impl Into<String>,
        rows: Vec<SequenceFeatureRow>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::SequenceAdd {
            key: key.into(),
            rows,
        })
    }

    pub fn sequence_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: usize,
        filters: Vec<FeatureFilter>,
    ) -> Result<Vec<SequenceFeatureRow>, ClientError> {
        match self
            .execute(Command::SequenceQuery {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                filters,
            })?
            .response
        {
            CommandResponse::SequenceRows { rows } => Ok(rows),
            response => Err(ClientError::UnexpectedResponse {
                operation: "sequence_query",
                response,
            }),
        }
    }

    /// The ranked windowed read over the cell snapshots stored under `key`. A read the key could
    /// not serve (an unknown name, a window over the bound) is an error naming why; a key with
    /// nothing to rank is an empty answer.
    pub fn cell_rank_query(
        &self,
        key: impl Into<String>,
        spec: crate::types::CellRankSpec,
    ) -> Result<crate::types::RankedCells, ClientError> {
        match self
            .execute(Command::CellRankQuery { key: key.into(), spec })?
            .response
        {
            CommandResponse::CellRanking { ranked } if ranked.refused.is_empty() => Ok(ranked),
            CommandResponse::CellRanking { ranked } => Err(ClientError::Status(ranked.refused)),
            response => Err(ClientError::UnexpectedResponse {
                operation: "cell_rank_query",
                response,
            }),
        }
    }

    /// Rolls the cell snapshots under `key` stamped strictly below `spec.older_than_ms` up into
    /// buckets, optionally capping each rolled snapshot, and says what it did. A request the key
    /// cannot serve is refused and nothing is written; rolling up again with the same arguments
    /// changes nothing.
    pub fn cell_rollup(
        &self,
        key: impl Into<String>,
        spec: crate::types::CellRollupSpec,
    ) -> Result<crate::types::CellRollupSummary, ClientError> {
        match self
            .execute(Command::CellRollup { key: key.into(), spec })?
            .response
        {
            CommandResponse::CellRollupReceipt {
                snapshots_before,
                snapshots_after,
                cells_before,
                cells_after,
                cells_dropped,
            } => Ok(crate::types::CellRollupSummary {
                snapshots_before,
                snapshots_after,
                cells_before,
                cells_after,
                cells_dropped,
            }),
            response => Err(ClientError::UnexpectedResponse {
                operation: "cell_rollup",
                response,
            }),
        }
    }

    /// The ranked windowed read over the cell snapshots of several keys that share one spec: the
    /// answer one key holding all of their snapshots would give. The keys must live on one shard
    /// (the request is routed by the first); `RankedCells::keys` says how many held a snapshot in
    /// the window, so an answer missing a key the shard did not hold is visible.
    pub fn cell_fan_in_rank_query(
        &self,
        keys: Vec<String>,
        spec: crate::types::CellRankSpec,
    ) -> Result<crate::types::RankedCells, ClientError> {
        match self.execute(Command::CellFanInRankQuery { keys, spec })?.response {
            CommandResponse::CellRanking { ranked } if ranked.refused.is_empty() => Ok(ranked),
            CommandResponse::CellRanking { ranked } => Err(ClientError::Status(ranked.refused)),
            response => Err(ClientError::UnexpectedResponse {
                operation: "cell_fan_in_rank_query",
                response,
            }),
        }
    }

    /// N independent ranked cell reads in one request, answered in request order. A read that
    /// cannot be served carries its reason in `refused` and does not fail the others. The keys
    /// must live on one shard (the request is routed by the first).
    pub fn cell_rank_batch_query(
        &self,
        queries: Vec<crate::types::CellRankBatchItem>,
    ) -> Result<Vec<crate::types::RankedCells>, ClientError> {
        match self.execute(Command::CellRankBatchQuery { queries })?.response {
            CommandResponse::CellRankings { rankings } => Ok(rankings),
            response => Err(ClientError::UnexpectedResponse {
                operation: "cell_rank_batch_query",
                response,
            }),
        }
    }

    pub fn sequence_batch_query(
        &self,
        queries: Vec<SequenceQuerySpec>,
    ) -> Result<Vec<(String, Vec<SequenceFeatureRow>)>, ClientError> {
        match self
            .execute(Command::SequenceBatchQuery { queries })?
            .response
        {
            CommandResponse::SequenceRowGroups { groups } => Ok(groups),
            response => Err(ClientError::UnexpectedResponse {
                operation: "sequence_batch_query",
                response,
            }),
        }
    }
}
