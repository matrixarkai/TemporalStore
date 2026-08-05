//! TemporalStoreTable IPS (in-place-series) methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn ips_add(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        instance: impl Into<Vec<u8>>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::IpsAdd {
            key: key.into(),
            timestamp_ms,
            instance: instance.into(),
        })
    }

    pub fn ips_add_with_options(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        instance: impl Into<Vec<u8>>,
        action_type: Option<u32>,
        table_id: Option<u64>,
        request_id: Option<String>,
    ) -> Result<bool, ClientError> {
        match self
            .execute(Command::IpsAddWithOptions {
                key: key.into(),
                timestamp_ms,
                instance: instance.into(),
                action_type,
                table_id,
                request_id,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_add_with_options",
                response,
            }),
        }
    }

    pub fn ips_query_last(
        &self,
        key: impl Into<String>,
        count: usize,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsQueryLast {
                key: key.into(),
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_query_last",
                response,
            }),
        }
    }

    pub fn ips_query_range(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsQueryRange {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_query_range",
                response,
            }),
        }
    }

    pub fn ips_load(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::IpsLoad {
                key: key.into(),
                points,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_load",
                response,
            }),
        }
    }

    pub fn ips_snapshot(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsSnapshot {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_snapshot",
                response,
            }),
        }
    }

    pub fn ips_snapshot_report(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<IpsSnapshotReport, ClientError> {
        match self
            .execute(Command::IpsSnapshotReport {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::IpsSnapshotReport { report } => Ok(report),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_snapshot_report",
                response,
            }),
        }
    }

    pub fn ips_stat(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<IpsStats, ClientError> {
        match self
            .execute(Command::IpsStat {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::IpsStats { stats } => Ok(stats),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_stat",
                response,
            }),
        }
    }

    pub fn ips_filter(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        action_type: Option<u32>,
        table_id: Option<u64>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsFilter {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                action_type,
                table_id,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_filter",
                response,
            }),
        }
    }

    pub fn ips_batch_query_last(
        &self,
        keys: Vec<String>,
        count: usize,
    ) -> Result<Vec<(String, Vec<FeaturePoint>)>, ClientError> {
        match self
            .execute(Command::IpsBatchQueryLast { keys, count })?
            .response
        {
            CommandResponse::FeaturePointGroups { groups } => Ok(groups),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_batch_query_last",
                response,
            }),
        }
    }

    pub fn ips_remove(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
    ) -> Result<bool, ClientError> {
        match self
            .execute(Command::IpsRemove {
                key: key.into(),
                timestamp_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_remove",
                response,
            }),
        }
    }

    pub fn ips_delete(&self, key: impl Into<String>) -> Result<bool, ClientError> {
        match self
            .execute(Command::IpsDelete { key: key.into() })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_delete",
                response,
            }),
        }
    }

    pub fn ips_count(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::IpsCount {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_count",
                response,
            }),
        }
    }

    pub fn ips_query_range_with_options(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        action_type: Option<u32>,
        table_id: Option<u64>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::IpsQueryRangeWithOptions {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                action_type,
                table_id,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "ips_query_range_with_options",
                response,
            }),
        }
    }
}
