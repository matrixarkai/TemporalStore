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

    pub fn feature_query_cpp_filters(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: &[String],
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        let filters = parse_cpp_feature_filters(filters.iter().map(String::as_str))
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
