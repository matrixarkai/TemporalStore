//! TemporalStoreTable risk methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn risk_increment(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskIncrement {
            key: key.into(),
            timestamp_ms,
            amount,
        })
    }

    pub fn risk_increment_with_options(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskIncrementWithOptions {
            key: key.into(),
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        })
    }

    pub fn risk_change_add(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        value: impl Into<Vec<u8>>,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskChangeAdd {
            key: key.into(),
            timestamp_ms,
            value: value.into(),
            precision_ms,
            ttl_ms,
        })
    }

    pub fn risk_count(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskCount {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_count",
                response,
            }),
        }
    }

    pub fn risk_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskQuery {
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_query",
                response,
            }),
        }
    }

    pub fn risk_detail(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::RiskDetail {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_detail",
                response,
            }),
        }
    }

    pub fn risk_family_set(
        &self,
        family: RiskFamily,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskSet {
            family,
            key: key.into(),
            timestamp_ms,
            amount,
        })
    }

    pub fn risk_family_query(
        &self,
        family: RiskFamily,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskFamilyQuery {
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
                operation: "risk_family_query",
                response,
            }),
        }
    }

    pub fn risk_family_set_and_get(
        &self,
        family: RiskFamily,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::RiskSetAndGet {
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
                operation: "risk_family_set_and_get",
                response,
            }),
        }
    }

    pub fn risk_fol_set(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        occur_time_ms: u64,
        ttl_ms: u64,
        fol_type: RiskFolType,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::RiskFolSet {
            key: key.into(),
            value: value.into(),
            occur_time_ms,
            ttl_ms,
            fol_type,
        })
    }

    pub fn risk_fol_query(&self, key: impl Into<String>) -> Result<Option<Vec<u8>>, ClientError> {
        match self
            .execute(Command::RiskFolQuery { key: key.into() })?
            .response
        {
            CommandResponse::Bytes { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_fol_query",
                response,
            }),
        }
    }

    pub fn risk_manager(
        &self,
        key: impl Into<String>,
    ) -> Result<Vec<(String, Vec<u8>)>, ClientError> {
        match self
            .execute(Command::RiskManager { key: key.into() })?
            .response
        {
            CommandResponse::HashEntries { entries } => Ok(entries),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_manager",
                response,
            }),
        }
    }

    pub fn risk_debug(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<(String, Vec<u8>)>, ClientError> {
        match self
            .execute(Command::RiskDebug {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::HashEntries { entries } => Ok(entries),
            response => Err(ClientError::UnexpectedResponse {
                operation: "risk_debug",
                response,
            }),
        }
    }
}
