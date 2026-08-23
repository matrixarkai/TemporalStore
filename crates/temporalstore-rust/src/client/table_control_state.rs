// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreTable control_state methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn control_state_increment(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateIncrement {
            key: key.into(),
            timestamp_ms,
            amount,
        })
    }

    pub fn control_state_increment_with_options(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateIncrementWithOptions {
            key: key.into(),
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        })
    }

    pub fn control_state_change_add(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        value: impl Into<Vec<u8>>,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateChangeAdd {
            key: key.into(),
            timestamp_ms,
            value: value.into(),
            precision_ms,
            ttl_ms,
        })
    }

    pub fn control_state_count(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::ControlStateCount {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_count",
                response,
            }),
        }
    }

    pub fn control_state_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::ControlStateQuery {
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_query",
                response,
            }),
        }
    }

    pub fn control_state_detail(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::ControlStateDetail {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_detail",
                response,
            }),
        }
    }

    pub fn control_state_family_set(
        &self,
        family: ControlStateFamily,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateSet {
            family,
            key: key.into(),
            timestamp_ms,
            amount,
        })
    }

    pub fn control_state_family_query(
        &self,
        family: ControlStateFamily,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::ControlStateFamilyQuery {
                family,
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_family_query",
                response,
            }),
        }
    }

    pub fn control_state_family_set_and_get(
        &self,
        family: ControlStateFamily,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::ControlStateSetAndGet {
                family,
                key: key.into(),
                timestamp_ms,
                amount,
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_family_set_and_get",
                response,
            }),
        }
    }

    /// Full-conformance control-state atomic increment-then-read (`HSETANDGET`
    /// analog) with precision bucketing, per-key TTL, and UUID idempotency.
    /// A repeated `uuid` within the dedup window is a no-op write that still
    /// returns the current windowed aggregate, so at-least-once queue replays do
    /// not double-count.
    #[allow(clippy::too_many_arguments)]
    pub fn control_state_family_set_and_get_with_options(
        &self,
        family: ControlStateFamily,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
        uuid: Option<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::ControlStateSetAndGetWithOptions {
                family,
                key: key.into(),
                timestamp_ms,
                amount,
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
                precision_ms,
                ttl_ms,
                uuid,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_family_set_and_get_with_options",
                response,
            }),
        }
    }

    pub fn control_state_selection_set(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        occur_time_ms: u64,
        ttl_ms: u64,
        selection_type: ControlStateSelectionType,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateSelectionSet {
            key: key.into(),
            value: value.into(),
            occur_time_ms,
            ttl_ms,
            selection_type,
        })
    }

    pub fn control_state_selection_query(&self, key: impl Into<String>) -> Result<Option<Vec<u8>>, ClientError> {
        match self
            .execute(Command::ControlStateSelectionQuery { key: key.into() })?
            .response
        {
            CommandResponse::Bytes { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_selection_query",
                response,
            }),
        }
    }

    pub fn control_state_manager(
        &self,
        key: impl Into<String>,
    ) -> Result<Vec<(String, Vec<u8>)>, ClientError> {
        match self
            .execute(Command::ControlStateManager {
                key: key.into(),
                op_type: None,
                field_list: Vec::new(),
                start_offset: String::new(),
                end_offset: String::new(),
                is_distinct: false,
            })?
            .response
        {
            CommandResponse::HashEntries { entries } => Ok(entries),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_manager",
                response,
            }),
        }
    }

    pub fn control_state_debug(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<(String, Vec<u8>)>, ClientError> {
        match self
            .execute(Command::ControlStateDebug {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::HashEntries { entries } => Ok(entries),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_debug",
                response,
            }),
        }
    }
}
