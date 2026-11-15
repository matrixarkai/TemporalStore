// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};

use crate::types::ShardId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCommandState {
    pub config: HashMap<String, String>,
    pub keyspace: HashSet<String>,
    pub master: Option<(String, String)>,
    pub authenticated: bool,
    pub loaded_shard_id: Option<ShardId>,
    /// Set by QUIT: the connection loop closes after writing the reply.
    pub closing: bool,
    /// CLIENT SETNAME value (empty = unset) and the per-connection CLIENT ID.
    pub client_name: Vec<u8>,
    pub client_id: u64,
}

static NEXT_CLIENT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl Default for RedisCommandState {
    fn default() -> Self {
        let mut config = HashMap::new();
        config.insert("requirepass".to_string(), String::new());
        config.insert("maxmemory".to_string(), "0".to_string());
        config.insert("maxmemory-policy".to_string(), "noeviction".to_string());
        Self {
            config,
            keyspace: HashSet::new(),
            master: None,
            authenticated: false,
            loaded_shard_id: None,
            closing: false,
            client_name: Vec::new(),
            client_id: NEXT_CLIENT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }
}
