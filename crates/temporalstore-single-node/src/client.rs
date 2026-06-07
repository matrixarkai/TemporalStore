use crate::http::{get_json, post_json, HttpError};
use crate::meta::GetShardResponse;
use crate::types::{BatchExecuteRequest, BatchExecuteResponse, ExecuteRequest, ExecuteResponse};

#[derive(Debug, Clone)]
pub struct TemporalStoreClient {
    proxy_addr: String,
}

impl TemporalStoreClient {
    pub fn new(proxy_addr: impl Into<String>) -> Self {
        Self {
            proxy_addr: proxy_addr.into(),
        }
    }

    pub fn execute(&self, request: ExecuteRequest) -> Result<ExecuteResponse, HttpError> {
        post_json(&self.proxy_addr, "/execute", &request)
    }

    pub fn batch_execute(
        &self,
        request: BatchExecuteRequest,
    ) -> Result<BatchExecuteResponse, HttpError> {
        post_json(&self.proxy_addr, "/batch_execute", &request)
    }

    pub fn get_shard(&self, shard_id: u64) -> Result<GetShardResponse, HttpError> {
        get_json(&self.proxy_addr, &format!("/shards/{shard_id}"))
    }
}
