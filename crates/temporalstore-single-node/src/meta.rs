use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::types::{ShardId, Status};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLocation {
    pub shard_id: ShardId,
    pub server_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardRequest {
    pub shard_id: ShardId,
    pub server_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetShardResponse {
    pub status: Status,
    pub location: Option<ShardLocation>,
}

#[derive(Debug, Default, Clone)]
pub struct SingleNodeMeta {
    shards: Arc<RwLock<HashMap<ShardId, ShardLocation>>>,
}

impl SingleNodeMeta {
    pub fn register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        self.shards.write().expect("meta lock poisoned").insert(
            request.shard_id,
            ShardLocation {
                shard_id: request.shard_id,
                server_addr: request.server_addr,
            },
        );
        RegisterShardResponse {
            status: Status::ok(),
        }
    }

    pub fn get(&self, shard_id: ShardId) -> GetShardResponse {
        let location = self
            .shards
            .read()
            .expect("meta lock poisoned")
            .get(&shard_id)
            .cloned();
        GetShardResponse {
            status: if location.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not registered")
            },
            location,
        }
    }
}
