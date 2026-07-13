use std::collections::{HashMap, HashSet};

use crate::types::ShardId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCommandState {
    pub config: HashMap<String, String>,
    pub keyspace: HashSet<String>,
    pub master: Option<(String, String)>,
    pub authenticated: bool,
    pub loaded_shard_id: Option<ShardId>,
    pub total_commands_processed: u64,
    pub rejected_commands: u64,
    pub open_source_rejected_commands: u64,
    pub unsupported_commands: u64,
}

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
            total_commands_processed: 0,
            rejected_commands: 0,
            open_source_rejected_commands: 0,
            unsupported_commands: 0,
        }
    }
}
