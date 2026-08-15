// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::Deserialize;
use serde_json::Value;
use std::borrow::Cow;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Command {
    pub op: String,
    pub key: Option<String>,
    pub field: Option<String>,
    pub value: Option<String>,
    pub entries: Option<Vec<HashEntry>>,
    pub entries_compact: Option<Vec<[String; 3]>>,
    pub append_options: Option<Value>,
    pub record: Option<Value>,
    pub records: Option<Vec<Value>>,
    pub record_type: Option<String>,
    pub tenant_hash: Option<u64>,
    pub record_id: Option<String>,
    pub record_ids: Option<Vec<String>>,
    pub count_key: Option<String>,
    pub record_hash_key: Option<String>,
    pub shard_size: Option<u64>,
    pub record_types: Option<Vec<String>>,
    pub secondary_index_groups: Option<Vec<Vec<String>>>,
    pub selected_node_hashes: Option<Vec<u64>>,
    pub scope: Option<Value>,
    pub metaserver: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub request_timeout_ms: Option<i32>,
    pub io_timeout_ms: Option<i32>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct HashEntry {
    pub key: String,
    pub field: String,
    pub value: Option<String>,
    pub route_json: Option<String>,
    pub storage_route: Option<Value>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct HashEntryRef<'a> {
    pub key: &'a str,
    pub field: &'a str,
    pub value: &'a str,
    pub route_json: Cow<'a, str>,
}
