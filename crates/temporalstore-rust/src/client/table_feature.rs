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
    /// `(samples written, cells created, cells merged)`. A request that disagrees with the
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
                samples_written,
                cells_created,
                cells_merged,
                ..
            } => Ok((samples_written, cells_created, cells_merged)),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_merge_cells",
                response,
            }),
        }
    }

    /// The same append, sent so that sending it again is safe: a key that already applied
    /// `retry_token` answers the first application's counts with `replayed` set and writes
    /// nothing, so a client that lost the answer can resend without summing a lane twice. A key
    /// remembers its last 16 tokens; zero is a token like any other.
    pub fn feature_merge_cells_with_token(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
        retry_token: u64,
    ) -> Result<crate::types::CellMergeSummary, ClientError> {
        match self
            .execute(Command::FeatureAppendWithPolicy {
                key: key.into(),
                points,
                policy: FeatureWritePolicy::MergeReduce,
                retry_token: Some(retry_token),
            })?
            .response
        {
            CommandResponse::CellMergeReceipt {
                samples_written,
                cells_created,
                cells_merged,
                replayed,
            } => Ok(crate::types::CellMergeSummary {
                samples_written,
                cells_created,
                cells_merged,
                replayed,
            }),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_merge_cells_with_token",
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

    /// The ranked windowed read over the cell samples stored under `key`. A read the key could
    /// not serve (an unknown name, a window over the bound) is an error naming why; a key with
    /// nothing to rank is an empty answer.
    pub fn window_reduce_rank(
        &self,
        key: impl Into<String>,
        spec: crate::types::WindowReduceRankSpec,
    ) -> Result<crate::types::RankedCells, ClientError> {
        match self
            .execute(Command::WindowReduceRank { key: key.into(), spec })?
            .response
        {
            CommandResponse::WindowReduceRanking { ranked } if ranked.refused.is_empty() => Ok(ranked),
            CommandResponse::WindowReduceRanking { ranked } => Err(ClientError::Status(ranked.refused)),
            response => Err(ClientError::UnexpectedResponse {
                operation: "window_reduce_rank",
                response,
            }),
        }
    }

    /// Rolls the cell samples under `key` stamped strictly below `spec.older_than_ms` up into
    /// buckets, optionally capping each rolled sample, and says what it did. A request the key
    /// cannot serve is refused and nothing is written; rolling up again with the same arguments
    /// changes nothing.
    pub fn window_reduce_rollup(
        &self,
        key: impl Into<String>,
        spec: crate::types::WindowReduceRollupSpec,
    ) -> Result<crate::types::WindowReduceRollupSummary, ClientError> {
        match self
            .execute(Command::WindowReduceRollup { key: key.into(), spec })?
            .response
        {
            CommandResponse::WindowReduceRollupReceipt {
                samples_before,
                samples_after,
                cells_before,
                cells_after,
                cells_dropped,
            } => Ok(crate::types::WindowReduceRollupSummary {
                samples_before,
                samples_after,
                cells_before,
                cells_after,
                cells_dropped,
            }),
            response => Err(ClientError::UnexpectedResponse {
                operation: "window_reduce_rollup",
                response,
            }),
        }
    }

    /// Removes cells from the samples under `key` by time window, member set and predicate, and
    /// says what it removed. A request the key cannot serve is refused and nothing is written; a
    /// request that matches no cell writes nothing and reports zero removed.
    pub fn window_reduce_remove(
        &self,
        key: impl Into<String>,
        spec: crate::types::WindowReduceRemoveSpec,
    ) -> Result<crate::types::WindowReduceRemoveSummary, ClientError> {
        match self
            .execute(Command::WindowReduceRemove { key: key.into(), spec })?
            .response
        {
            CommandResponse::WindowReduceRemoveReceipt {
                samples_before,
                samples_after,
                cells_before,
                cells_removed,
            } => Ok(crate::types::WindowReduceRemoveSummary {
                samples_before,
                samples_after,
                cells_before,
                cells_removed,
            }),
            response => Err(ClientError::UnexpectedResponse {
                operation: "window_reduce_remove",
                response,
            }),
        }
    }

    /// The ranked windowed read over the cell samples of several keys that share one spec: the
    /// answer one key holding all of their samples would give. The keys must live on one shard
    /// (the request is routed by the first); `RankedCells::keys` says how many held a sample in
    /// the window, so an answer missing a key the shard did not hold is visible.
    pub fn window_reduce_fan_in_rank(
        &self,
        keys: Vec<String>,
        spec: crate::types::WindowReduceRankSpec,
    ) -> Result<crate::types::RankedCells, ClientError> {
        match self.execute(Command::WindowReduceFanInRank { keys, spec })?.response {
            CommandResponse::WindowReduceRanking { ranked } if ranked.refused.is_empty() => Ok(ranked),
            CommandResponse::WindowReduceRanking { ranked } => Err(ClientError::Status(ranked.refused)),
            response => Err(ClientError::UnexpectedResponse {
                operation: "window_reduce_fan_in_rank",
                response,
            }),
        }
    }

    /// N independent ranked cell reads in one request, answered in request order. A read that
    /// cannot be served carries its reason in `refused` and does not fail the others. The keys
    /// must live on one shard (the request is routed by the first).
    pub fn window_reduce_rank_batch(
        &self,
        queries: Vec<crate::types::WindowReduceRankBatchItem>,
    ) -> Result<Vec<crate::types::RankedCells>, ClientError> {
        match self.execute(Command::WindowReduceRankBatch { queries })?.response {
            CommandResponse::WindowReduceRankings { rankings } => Ok(rankings),
            response => Err(ClientError::UnexpectedResponse {
                operation: "window_reduce_rank_batch",
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
